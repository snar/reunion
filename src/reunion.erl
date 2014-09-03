-module(reunion).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
	code_change/3]).
-export([start_link/0]).
-export([is_locally_inserted/2, is_locally_removed/2]).
-export([check_inconsistencies/0, report_inconsistency/4]).

-define(TABLE, ?MODULE).
-define(DEQUEUE_TIMEOUT, 1000).
-define(WAIT_TIMEOUT, 5000).
-define(EXPIRE_TIMEOUT, case net_kernel:get_net_ticktime() of
	ignored -> 60+1;
	{ongoing_change_to, Tick} -> Tick+1;
	Tick -> Tick+1
end). % seconds
-define(LOCK, {?MODULE, lock}).
-define(DEFAULT_METHOD, {reunion_lib, merge_only, []}).
-define(DONE, {?MODULE, merge_done}).
-define(RESUBSCRIBE_TIMEOUT, 100).
-ifdef(DEBUG).
-define(debug(Fmt, Arg), error_logger:info_msg(Fmt, Arg)).
-else.
-define(debug(Fmt, Arg), ok).
-endif.

-type action() :: {'write_local', any()} | {'write_remote', any()} |
	{'delete_local', any()} | {'delete_remote', any()}.
-export_type([action/0]).

-record(qentry, {table, key, expires, op, created = os:timestamp()}).
-record(state, {db, queue, mode, tables = sets:new(), exptimer, pingtimer}).
-record(s0, {table, type, attributes, module, function, xargs, remote,
	modstate}).

start_link() ->
	case wait_mnesia(10) of
		ok ->
			gen_server:start_link({local, ?MODULE}, ?MODULE, [[]], []);
		_ ->
			{error, {mnesia, not_running}}
	end.

init(_Args) ->
	{ok, _} = mnesia:subscribe(system),
	{ok, _} = mnesia:subscribe({table, schema, detailed}),
	Db = ets:new(?TABLE, [set, named_table]),
	State = lists:foldl(fun
		(schema, Acc) ->
			Acc;
		(T, Acc) ->
			Attrs = mnesia:table_info(T, all),
			case {should_track(T, Attrs), sets:is_element(T, Acc#state.tables)}
				of
				{true, true} ->
					% this table is already tracked, most likely fragment
					Acc;
				{true, _} ->
					track_table(T, Acc);
				{false, false} ->
					Acc
			end
		end, #state{tables=sets:new()}, mnesia:system_info(tables)),
	Mode = case mnesia:system_info(db_nodes) --
		mnesia:system_info(running_db_nodes) of
		[] -> queue;
		_  ->
			case application:get_env(reunion, reconnect) of
				{ok, never} -> ok;
				_ -> self() ! {timeout, undefined, ping}
			end,
			store
	end,
	error_logger:info_msg("~p (init): starting in ~p mode, tracking ~p~n",
		[?MODULE, Mode, sets:to_list(State#state.tables)]),
	Expire = erlang:start_timer(?DEQUEUE_TIMEOUT, ?MODULE, expire),
	{ok, State#state{db=Db, mode=Mode, queue=queue:new(), exptimer=Expire}}.

handle_call(_Any, _From, State) ->
	{reply, {error, badcall}, State}.

handle_cast(_Any, State) ->
	{noreply, State}.

handle_info({mnesia_table_event, {write, schema, {schema, schema, _Attrs}, _,
	_ActId}}, State) ->
	{noreply, State};
handle_info({mnesia_table_event, {write, schema, {schema, Table, Attrs}, _,
	_ActId}}, State) ->
	case {should_track(Table, Attrs),
		sets:is_element(Table, State#state.tables)} of
		{true, true} ->
			?debug("reunion(write, schema): ~p is already tracked", [Table]),
			{noreply, State};
		{true, false} ->
			?debug("reunion(write, schema): calling track_table(~p)", [Table]),
			{noreply, track_table(Table, State)};
		{false, true} ->
			?debug("reunion(write, schema): calling untrack_table(~p)", 
				[Table]),
			{noreply, untrack_table(Table, State)};
		{false, false} ->
			?debug("reunion(write, schema): ~p is not tracked", [Table]),
			{noreply, State}
	end;
handle_info({mnesia_table_event, {delete, schema, {schema, Table, _Attrs}, _,
	_ActId}}, State) ->
	case sets:is_element(Table, State#state.tables) of
		true ->
			{noreply, untrack_table(Table, State)};
		false ->
			{noreply, State}
	end;
handle_info({mnesia_table_event, {write, Table, Record, [], _ActId}}, State) ->
	Nq = case sets:is_element(Table, State#state.tables) of
		false ->
			?debug("reunion(write new): table ~p is not tracked~n", [Table]),
			State#state.queue;
		true  ->
			case State#state.mode of
				store ->
					?debug("reunion(store): storing {~p, ~p}~n",
						[Table, element(2, Record)]),
					ets:insert(?TABLE, {{Table, element(2, Record)}, 'insert',
						os:timestamp()}),
					State#state.queue;
				queue ->
					?debug("reunion(~p): queueing {~p, ~p}, expires: ~p~n",
							[State#state.mode, Table, element(2, Record),
							expires()]),
					queue:in(#qentry{table=Table, key=element(2, Record),
						expires=expires(), op='insert', created=os:timestamp()},
						State#state.queue)
			end
	end,
	{noreply, State#state{queue=Nq}};
handle_info({mnesia_table_event, {write, _Table, _Record, _NonEmptyList, _Act}},
	State) ->
	% this is update of already existing key, may ignore
	?debug("reunion(update, ignored): table ~p, ~p~n", [_Table, _Record]),
	{noreply, State};
handle_info({mnesia_table_event, {delete, Table, {Table, Key}, _Value, _ActId}},
	State) ->
	Nq = case sets:is_element(Table, State#state.tables) of
		true ->
			case State#state.mode of
				store ->
					ets:insert(?TABLE, {{Table, Key}, 'delete',
						os:timestamp()}),
					State#state.queue;
				queue ->
					queue:in(#qentry{table=Table, key=Key, expires=expires(),
						op='insert', created=os:timestamp()}, State#state.queue)
			end;
		false ->
			State#state.queue
	end,
	{noreply, State#state{queue=Nq}};
handle_info({mnesia_table_event, {delete, Table, Record, _Old, _ActId}},
	State) ->
	Key = element(2, Record),
	Nq = case sets:is_element(Table, State#state.tables) of
		true ->
			case State#state.mode of
				store ->
					ets:insert(?TABLE, {{Table, Key}, 'delete',
						os:timestamp()}),
					State#state.queue;
				queue ->
					queue:in(#qentry{table=Table, key=Key, expires=expires(),
						op='delete', created=os:timestamp()}, State#state.queue)
			end;
		false ->
			State#state.queue
	end,
	{noreply, State#state{queue=Nq}};
handle_info({mnesia_system_event, {mnesia_up, Node}}, State) when
	Node == node() ->
	error_logger:info_msg("reunion: got mnesia_up for local node ~p, ignore",
		[node()]),
	{noreply, State};
handle_info({mnesia_system_event, {mnesia_up, Node}}, State) when
	State#state.mode == store ->
	{Mode, Nq} = case nextmode() of
		store ->
			{store, State#state.queue};
		queue ->
			Now = os:timestamp(),
			N = ets:foldl(fun({{Tab, Key}, Op, Happened} = _E, Q) ->
				Exp = expires(Happened),
				case expires(Happened) < Now of
					true ->
						?debug("reunion(store->queue): not requeueing ~p~n",
							[_E]),
						Q;
					false ->
						?debug("reunion(store->queue): requeueing ~p~n", [_E]),
						queue:in(#qentry{table=Tab, key=Key, op=Op,
							expires=Exp}, Q)
				end end, State#state.queue, ?TABLE),
			ets:delete_all_objects(?TABLE),
			{queue, N}
	end,
	error_logger:info_msg("~p: got mnesia_up ~p in store mode, next mode: ~p",
		[?MODULE, Node, Mode]),
	{noreply, State#state{mode=Mode, queue=Nq}};
handle_info({mnesia_system_event, {mnesia_down, Node}}, State) when
	node() == Node ->
	error_logger:info_msg("~p: got mnesia_down for local node, stop",
		[?MODULE]),
	{stop, normal, State};
handle_info({mnesia_system_event, {mnesia_down, Node}}, State) when
	State#state.mode == queue ->
	% mirror all queued entries to ets
	queue_mirror(State#state.queue),
	Ping = schedule_ping(),
	error_logger:info_msg("~p: got mnesia_down ~p in queue mode, switching "
		"to store, timer: ~p", [?MODULE, Node, Ping]),
	{noreply, State#state{mode=store, pingtimer=Ping}};
handle_info({mnesia_system_event, {inconsistent_database,
	running_partitioned_network, Node}}, State) ->
	error_logger:info_msg("~p: Inconsistency (running_partitioned_network) "
		"with ~p~n", [?MODULE, Node]),
	case application:get_env(?MODULE, delay, 0) of
		0 -> ok;
		Value ->
			?debug("reunion: sleeping ~p before acquiring lock", [Value]),
			timer:sleep(Value)
	end,
	global:trans({?LOCK, self()},
		fun() ->
			?debug("~p: have global lock. mnesia locks: ~p",
				[?MODULE, mnesia_locker:get_held_locks()]),
			?debug("~p: nodes: ~p,~n   running: ~p,~n   ~p messages: ~p~n",
				[?MODULE, mnesia:system_info(db_nodes),
					mnesia:system_info(running_db_nodes),
					process_info(self(), message_queue_len),
					process_info(self(), messages)]),
			stitch_together(Node)
		end),
	{noreply, State};
handle_info({mnesia_system_event, {inconsistent_database,
	starting_partitioned_network, Node}}, State) ->
	% this is recovery message sent after merge.
	error_logger:info_msg("~p: starting_partitioned_network with ~p",
		[?MODULE, Node]),
	{noreply, State};
handle_info({mnesia_system_event, {inconsistent_database, Context, Node}},
	State) ->
	error_logger:info_msg("~p: mnesia inconsistent_database in ~p with ~p",
		[?MODULE, Context, Node]),
	{noreply, State};
handle_info({timeout, Ref, ping}, #state{pingtimer=Ref} = State) ->
	case mnesia:system_info(db_nodes) -- mnesia:system_info(running_db_nodes) of
		[] ->
			{noreply, State#state{pingtimer=undefined}};
		List ->
			spawn(fun() ->
				lists:foreach(fun(N) -> net_adm:ping(N) end, List)
			end),
			Ping = schedule_ping(),
			{noreply, State#state{pingtimer=Ping}}
	end;
handle_info(ping, State) ->
	case mnesia:system_info(db_nodes) -- mnesia:system_info(running_db_nodes) of
		[] ->
			ok;
		List ->
			spawn(fun() ->
				lists:foreach(fun(N) -> net_adm:ping(N) end, List) end),
			ok
	end,
	case State#state.pingtimer of
		undefined ->
			% will not setup timer if reconnect set to never
			{noreply, State#state{pingtimer=schedule_ping()}};
		_ ->
			{noreply, State}
	end;
handle_info({timeout, Ref, expire}, #state{exptimer=Ref} = State) ->
	ETimer = erlang:start_timer(?DEQUEUE_TIMEOUT, self(), expire),
	Now = os:timestamp(),
	Queue = dequeue(State#state.queue, Now),
	{noreply, State#state{queue=Queue, exptimer=ETimer}};
handle_info({timeout, _, {subscribe, T}}, #state{} = State) ->
	case {should_track(T), sets:is_element(T, State#state.tables)} of
		{false, false} ->
			{noreply, State};
		{true, false} ->
			{noreply, track_table(T, State)};
		{true, true} ->
			{noreply, State}
	end;
handle_info({mnesia_system_event,{mnesia_info, _, _}} = _E, State) ->
	error_logger:info_msg("unhandled mnesia event ~p", [_E]),
	{noreply, State};
handle_info({mnesia_system_event, {mnesia_down, _Node}}, State)
	when State#state.mode == store ->
	{noreply, State};
handle_info(Any, State) ->
	error_logger:info_msg("~p: unhandled info ~p~n", [?MODULE, Any]),
	{noreply, State}.

terminate(Reason, _State) ->
	error_logger:info_msg("~p: terminating (~p)", [?MODULE, Reason]),
	ok.

code_change(_Old, State, _Extra) ->
	{ok, State}.

schedule_ping() ->
	Exp = ?EXPIRE_TIMEOUT,
	case application:get_env(?MODULE, reconnect, Exp) of
		never -> undefined;
		Time when is_integer(Time) -> 
			erlang:start_timer(Time*1000, ?MODULE, ping);
		Bad -> 
			error_logger:info_msg("~p: bad reconnect value ~p", [?MODULE, Bad]),
			erlang:start_timer(Exp*1000, ?MODULE, ping)
	end.

track_table(Table, State) ->
	Pre = os:timestamp(),
	case mnesia:wait_for_tables([Table], ?WAIT_TIMEOUT) of
		ok ->
			Post = os:timestamp(),
			case mnesia:subscribe({table, Table, detailed}) of
				{ok, _} ->
					error_logger:info_msg("~p: started tracking ~p, "
						"elapsed:  ~p micros", [?MODULE, Table,
						timer:now_diff(Post, Pre)]),
					Ns = sets:add_element(Table, State#state.tables),
					track_fragments(Table, State#state{tables=Ns});
				{error, {not_active_local, Table}} ->
					erlang:start_timer(?RESUBSCRIBE_TIMEOUT, self(),
						{subscribe, Table}),
					State;
				{error, {no_exists, Table}} ->
					error_logger:info_msg("~p: table ~p disappeared while "
						"subscribe", [?MODULE, Table]),
					State;
				{error, Other} ->
					error_logger:info_msg("~p: error subscribing ~p: ~p",
						[?MODULE, Table, Other]),
					State
			end;
		{timeout, [Table]} ->
			erlang:start_timer(?RESUBSCRIBE_TIMEOUT, ?MODULE,
				{subscribe, Table}),
			State
	end.

track_fragments(Table, State) when is_atom(Table) ->
	case mnesia:activity(async_dirty, fun() ->
		mnesia:table_info(Table, frag_names) end, [], mnesia_frag) of
		[Table] -> State;
		[Table|Frags] ->
			track_fragments(Frags, State)
	end;
track_fragments([], State) -> State;
track_fragments([Fragment|Next], State) ->
	case {should_track(Fragment),
		sets:is_element(Fragment, State#state.tables)} of
		{true, true} ->
			track_fragments(Next, State);
		{true, false} ->
			track_fragments(Next, track_table(Fragment, State));
		{false, false} ->
			track_fragments(Next, State)
	end.

untrack_fragments(Table, State) when is_atom(Table) ->
	case mnesia:activity(async_dirty, fun() ->
		mnesia:table_info(Table, frag_names) end, [], mnesia_frag) of
		[Table] -> State;
		[Table|Frags] ->
			untrack_fragments(Frags, State)
	end;
untrack_fragments([], State) -> State;
untrack_fragments([Fragment|Next], State) ->
	case {should_track(Fragment),
		sets:is_element(Fragment, State#state.tables)} of
		{false, false} ->
			untrack_fragments(Next, State);
		{false, true} ->
			untrack_fragments(Next, untrack_table(Fragment, State));
		{true, true} ->
			untrack_fragments(Next, State)
	end.

untrack_table(Table, State) ->
	error_logger:info_msg("~p: stop tracking ~p", [?MODULE, Table]),
	mnesia:unsubscribe({table, Table, detailed}),
	Nq = case State#state.mode of
		queue ->
			unqueue(State#state.queue, Table);
		store ->
			ets:match_delete(?TABLE, {{Table, '_'}, '_', '_'}),
			State#state.queue
	end,
	Ns = sets:del_element(Table, State#state.tables),
	untrack_fragments(Table, State#state{queue=Nq, tables=Ns}).

should_track(T) ->
	try mnesia:table_info(T, all) of
		Attrs ->
			should_track(T, Attrs)
	catch
		_Error:_Code ->
			false
	end.

should_track(T, Attr) ->
	LocalContent = proplists:get_value(local_content, Attr),
	Type = proplists:get_value(type, Attr),
	AllNodes = proplists:get_value(disc_copies, Attr, []) ++
		proplists:get_value(ram_copies, Attr, []) ++
		proplists:get_value(disc_only_copies, Attr, []),
	Member   = lists:member(node(), AllNodes),
	Compare  = case proplists:get_value(frag_properties, Attr, []) of
		[] -> proplists:get_value(reunion_compare,
				proplists:get_value(user_properties, Attr, []));
		Frag ->
			case proplists:get_value(base_table, Frag) of
				T -> proplists:get_value(reunion_compare,
						proplists:get_value(user_properties, Attr, []));
				DiffT ->
					get_method(DiffT, default_method())
			end
	end,
	if
		LocalContent == true ->
			?debug("reunion: should not track ~p: local_content only~n", [T]),
			false;
		Member == false ->
			?debug("reunion: should not track ~p: this node does not have "
				"a copy", [T]),
			false;
		Type == bag ->  % sets and ordered_sets are ok
			?debug("reunion: should not track ~p: bag~n", [T]),
			false;
		length(AllNodes) == 1 ->
			?debug("reunion: should not track ~p: all_nodes ~p~n",
				[T, AllNodes]),
			false;
		Compare == ignore ->
			?debug("reunion: should not track ~p: reunion_compare is ignore~n",
				[T]),
			false;
		true ->
			?debug("reunion: should track ~p~n", [T]),
			true
	end.

expires() ->
	expires(os:timestamp()).

expires({M, S, Ms}) ->
	Exp = ?EXPIRE_TIMEOUT,
	case S + Exp >= 1000000 of
		true ->
			{M+1, S+Exp-1000000, Ms};
		false ->
			{M, S+Exp, Ms}
	end.

dequeue(Queue, Now) ->
	case queue:out(Queue) of
		{empty, Queue} -> Queue;
		{{value, #qentry{expires=Exp}}, _Q1} when Exp > Now ->
			Queue;
		{{value, #qentry{} = _Qe}, Q1} ->
			?debug("reunion(dequeue): expired ~p (~p)~n", [_Qe, Now]),
			dequeue(Q1, Now)
	end.

unqueue(Queue, Table) ->
	queue:filter(fun
		(#qentry{table=T} = _Qe) when T == Table ->
			?debug("reunion(unqueue/2): deleted ~p~n", [_Qe]),
			false;
		(#qentry{}) -> true
		end, Queue).

nextmode() ->
	case mnesia:system_info(db_nodes) -- mnesia:system_info(running_db_nodes) of
		[] -> queue;
		_  -> store
	end.

queue_mirror(Queue) ->
	case queue:out(Queue) of
		{empty, Queue} -> ok;
		{{value, #qentry{table=T, key=K, op=Op, created=C}}, Q1} ->
			?debug("reunion(queue_mirror): storing {~p, ~p}, ~p~n", [T, K, Op]),
			ets:insert(?TABLE, {{T, K}, Op, C}),
			queue_mirror(Q1)
	end.

stitch_together(Node) ->
	case lists:member(Node, mnesia:system_info(running_db_nodes)) of
		true ->
			error_logger:info_msg("~p: node ~p already running, "
				"not stitching~n", [?MODULE, Node]),
			ok;
		false ->
			pre_stitch_together(Node)
	end.

pre_stitch_together(Node) ->
	case rpc:call(Node, mnesia, system_info, [is_running]) of
		yes ->
			do_stitch_together(Node);
		Other ->
			error_logger:info_msg("~p: node ~p: mnesia not running (~p), not "
				"stitching~n", [?MODULE, Node, Other]),
			ok
	end.
			
do_stitch_together(Node) ->
	IslandB = case rpc:call(Node, mnesia, system_info, [running_db_nodes]) of
		{badrpc, Reason} ->
			error_logger:info_msg("~p: unable to ask mnesia:system_info("
				"running_db_nodes) on ~p: ~p", [?MODULE, Node, Reason]),
			[];
		Answer ->
			Answer
	end,
	TabsAndNodes = affected_tables(IslandB),
	Tables = [ T || {T, _} <- TabsAndNodes ],
	DefaultMethod = default_method(),
	TabMethods = [{T, Ns, get_method(T, DefaultMethod)} ||
		{T, Ns} <- TabsAndNodes],
	error_logger:info_msg("~p: calling connect_nodes(~p) with Tables = ~p",
		[?MODULE, Node, Tables]),
	mnesia_controller:connect_nodes([Node],
		fun(MergeF) ->
			case MergeF(Tables) of
				{merged, _, _} = Res ->
					error_logger:info_msg("~p: MergeF ret = ~p in ~p",
						[?MODULE, Res, self()]),
					stitch_tabs(TabMethods, Node),
					Res;
				Other ->
					error_logger:info_msg("~p: MergeF ret = ~p (nonstitch) "
						"in ~p", [?MODULE, Other, self()]),
					Other
			end
		end).

stitch_tabs(TabMethods, Node) ->
	[ do_stitch(TM, Node) || TM <- TabMethods ].

do_stitch({Tab, _Nodes, {M, F, Xargs}}, Node) ->
	Type  = case mnesia:table_info(Tab, type) of ordered_set -> set; S -> S end,
	Attrs = mnesia:table_info(Tab, attributes),
	{ok, Ms} = M:F(init, {Tab, Type, Attrs, Xargs}, Node),
	S0 = #s0{module = M, function=F, xargs=Xargs, table=Tab, type=Type,
		attributes = Attrs, remote = Node, modstate=Ms},
	try run_stitch(S0) of
		ok -> ok
	catch
		throw:?DONE -> ok
	end;
do_stitch({Tab, _Nodes, ignore}, _Node) ->
	error_logger:info_msg("~p: ignoring table ~p (configuration)",
		[?MODULE, Tab]),
	ok.

run_stitch(#s0{module=M, function=F, table=Tab, remote=Remote, type=Type,
	modstate=MSt}) ->
	LocalKeys = mnesia:dirty_all_keys(Tab),
	% usort used to remove duplicates
	Keys = lists:usort(lists:concat([LocalKeys, remote_keys(Remote, Tab)])),
	lists:foldl(fun(K, Sx) ->
		A = mnesia:dirty_read({Tab, K}),
		B = remote_object(Remote, Tab, K),
		case {A, B} of
			{[], []} ->
				% element is not present anymore
				Sx;
			{Aa, Aa} ->
				% elements are the same
				?debug("reunion(~p, ~p): same element ~p", [Tab, K, Aa]),
				Sx;
			{[Aa], []} when Type == set ->
				% remote element is not present
				case is_locally_inserted(Tab, K) of
					{true, LocalTime} ->
						case rpc:call(Remote, reunion, is_locally_removed,
							[Tab, K]) of
							false ->
								?debug("reunion(stitch ~p ~p ~p ~p): write "
									"remote (not removed)~n",
									[Tab, Type, A, B]),
								write(Remote, Aa);
							{true, RemoteTime} when LocalTime < RemoteTime ->
								?debug("reunion(stitch ~p ~p ~p ~p): delete "
									"local (remote timer won)~n",
									[Tab, Type, A, B]),
								delete(Aa);
							_ ->  % can be an error too
								?debug("reunion(stitch ~p ~p ~p ~p): write "
									"remote (error or timer)~n",
									[Tab, Type, A, B]),
								write(Remote, Aa)
						end;
					false ->
						?debug("reunion(stitch ~p ~p ~p ~p): delete local~n",
							[Tab, Type, A, B]),
						delete(Aa)
				end,
				Sx;
			{[], [Bb]} when Type == set ->
				% local element not present
				case is_locally_removed(Tab, K) of
					{true, LocalTime} ->
						case rpc:call(Remote, ?MODULE, is_locally_inserted,
							[Tab, K]) of
							false ->
								?debug("reunion(stitch ~p ~p ~p ~p): delete "
									"remote (not remotely inserted)~n",
									[Tab, Type, A, B]),
								delete(Remote, Bb);
							{true, RemoteTime} when LocalTime < RemoteTime ->
								?debug("reunion(stitch ~p ~p ~p ~p): write "
									"local (remote timer won)~n",
									[Tab, Type, A, B]),
								write(Bb);
							_ ->
								?debug("reunion(stitch ~p ~p ~p ~p): delete "
									"remote (error or timer)~n",
									[Tab, Type, A, B]),
								delete(Remote, Bb)
						end;
					false ->
						?debug("reunion(stitch ~p ~p ~p ~p): write local~n",
							[Tab, Type, A, B]),
						write(Bb)
				end,
				Sx;
			{A, B} ->
				Sn = case M:F(A, B, Sx) of
					{ok, Actions, Sr} ->
						?debug("reunion(stitch ~p ~p ~p ~p): ~p"
							"both~n", [Tab, Type, A, B]),
						do_actions(Actions, Remote),
						Sr;
					{inconsistency, Error, Sr} ->
						?debug("reunion(stitch ~p ~p ~p ~p): inconsistency "
							"~p~n", [Tab, Type, A, B, Error]),
						report_inconsistency(Remote, Tab, K, Error),
						Sr
				end,
				Sn
		end
		end, MSt, Keys),
	M:F(done, MSt, Remote),
	ok.

do_actions([], _) ->
	ok;
do_actions([{write_local, Ae}|Next], Remote) ->
	write(Ae), do_actions(Next, Remote);
do_actions({write_local, Ae}, _Remote) ->
	write(Ae);
do_actions([{delete_local, Ae}|Next], Remote) ->
	delete(Ae), do_actions(Next, Remote);
do_actions({delete_local, Ae}, _Remote) ->
	delete(Ae);
do_actions([{write_remote, Ae}|Next], Remote) ->
	write(Remote, Ae), do_actions(Next, Remote);
do_actions({write_remote, Ae}, Remote) ->
	write(Remote, Ae);
do_actions([{delete_remote, Ae}|Next], Remote) ->
	delete(Remote, Ae), do_actions(Next, Remote);
do_actions({delete_remote, Ae}, Remote) ->
	delete(Remote, Ae).

affected_tables(IslandB) ->
	IslandA = mnesia:system_info(running_db_nodes),
	Tables = mnesia:system_info(tables) -- [schema],
	lists:foldl(fun(T, Acc) ->
		Nodes = mnesia:table_info(T, all_nodes),
		case {intersection(IslandA, Nodes), intersection(IslandB, Nodes)} of
			{[_|_], [_|_]} ->
				[{T, Nodes}|Acc];
			_ -> Acc
		end end, [], Tables).

write(Remote, A) ->
	rpc:call(Remote, mnesia, dirty_write, [A]).

write(A) when is_list(A) ->
	lists:foreach(fun(E) -> mnesia:dirty_write(E) end, A);
write(A) ->
	mnesia:dirty_write(A).

delete(Remote, A) ->
	rpc:call(Remote, mnesia, dirty_delete_object, [A]).

delete(A) when is_list(A) ->
	lists:foreach(fun(E) -> mnesia:dirty_delete_object(E) end, A);
delete(A) ->
	mnesia:dirty_delete_object(A).

remote_keys(Remote, Tab) ->
	case rpc:call(Remote, mnesia, dirty_all_keys, [Tab]) of
		{badrpc, Reason} ->
			error_logger:error_msg("?p: error querying dirty_all_keys(~p) "
				"on ~p:~n  ~p", [?MODULE, Tab, Remote, Reason]),
			mnesia:abort({badrpc, Remote, Reason});
		Keys -> Keys
	end.

remote_object(Remote, Tab, Key) ->
	case rpc:call(Remote, mnesia, dirty_read, [Tab, Key]) of
		{badrpc, Reason} ->
			error_logger:error_msg("?p: error fetching {~p, ~p} on ~p:~n  ~p",
				[?MODULE, Tab, Key, Remote, Reason]),
			mnesia:abort({badrpc, Remote, Reason});
		Object -> Object
	end.

is_locally_inserted(Tab, Key) ->
	case ets:lookup(?TABLE, {Tab, Key}) of
		[] ->
			?debug("reunion(is_locally_inserted): {~p, ~p} not in ets~n",
				[Tab, Key]),
			false;
		[{{Tab, Key}, 'insert', T}] ->
			?debug("reunion(is_locally_inserted): {~p, ~p} was inserted~n",
				[Tab, Key]),
			{true, T};
		[{{Tab, Key}, 'delete', _}] ->
			?debug("reunion(is_locally_inserted): {~p, ~p} was deleted~n",
				[Tab, Key]),
			false
	end.

is_locally_removed(Tab, Key) ->
	case ets:lookup(?TABLE, {Tab, Key}) of
		[] ->
			?debug("reunion(is_locally_removed): {~p, ~p} not in ets~n",
				[Tab, Key]),
			false;
		[{{Tab, Key}, 'insert', _}] ->
			?debug("reunion(is_locally_removed): {~p, ~p} was inserted~n",
				[Tab, Key]),
			false;
		[{{Tab, Key}, 'delete', T}] ->
			?debug("reunion(is_locally_removed): {~p, ~p} was deleted~n",
				[Tab, Key]),
			{true, T}
	end.

check_inconsistencies() ->
	lists:foreach(fun({{?MODULE, inconsistency, Remote, Tab, Key}, _}) ->
		case lists:member(Remote, nodes()) of
			false -> ok;
			true  ->
				case {mnesia:dirty_read(Tab, Key),
					rpc:call(Remote, mnesia, dirty_read, [Tab, Key])} of
					{A, A} ->
						alarm_handler:clear_alarm({?MODULE, inconsistency,
							Remote, Tab, Key});
					_ -> ok
				end
		end;
		(_) -> ok end, alarm_handler:get_alarms()).

report_inconsistency(Remote, Tab, Key, Error) ->
	alarm_handler:set_alarm({{?MODULE, inconsistency, Remote, Tab, Key},
		Error}).

default_method() ->
	get_env(default_method, ?DEFAULT_METHOD).

get_method(Table, Default) ->
	try mnesia:read_table_property(Table, reunion_compare) of
		{reunion_compare, Method} -> Method
	catch
		exit:_ ->
			Default
	end.

get_env(Env, Default) ->
	case application:get_env(Env) of
		undefined -> Default;
		{ok, undefined} -> Default;
		{ok, Value} -> Value
	end.

intersection(A, B) -> A -- (A -- B).

wait_mnesia(N) ->
	case mnesia:system_info(is_running) of
		yes -> ok;
		_ ->
			case N > 0 of
				true ->
					timer:sleep(100),
					wait_mnesia(N-1);
				false ->
					{error, not_running}
			end
	end.
