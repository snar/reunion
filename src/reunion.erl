-module(reunion).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
	code_change/3]).
-export([start_link/0]).
-export([handle_remote/2, is_locally_inserted/2, is_remotely_inserted/3]).
-export([report_inconsistency/4, try_recover/1, try_recover/0]).

-define(TABLE, ?MODULE).
-define(DEQUEUE_TIMEOUT, 1000).
-define(EXPIRE_TIMEOUT, 60). % seconds
-define(LOCK, {?MODULE, lock}).
-define(DEFAULT_METHOD, {reunion_lib, merge_only, []}).
-define(DONE, {?MODULE, merge_done}).
-define(DEBUG, 1).
-ifdef(DEBUG).
-define(debug(Fmt, Arg), io:format(Fmt, Arg)).
-else.
-define(debug(Fmt, Arg), ok).
-endif.

-record(qentry, {table, key, expires}).
-record(state, {db, queue, mode, tables = sets:new()}).
-record(s0, {table, type, attributes, module, function, xargs, remote, 
	modstate}).

start_link() -> 
	gen_server:start_link({local, ?MODULE}, ?MODULE, [[]], []).

try_recover(Node) -> 
	case lists:member(Node, nodes()) of 
		true -> 
			error_logger:info_msg("~p: simulating inconsistent database "
				"event: node ~p is running", [Node]),
			whereis(?MODULE) ! {mnesia_system_event, {inconsistent_database,
				local, Node}};
		false -> 
			error_logger:info_msg("ignoring node ~p: not connected", [Node])
	end.


try_recover() -> 
	case mnesia:system_info(db_nodes) -- mnesia:system_info(running_db_nodes) of
		[] -> 
			ok;
		Nodes -> 
			lists:foreach(fun(F) -> try_recover(F) end, Nodes)
	end.

init(_Args) -> 
	Node = node(),
	{ok, Node} = mnesia:subscribe(system),
	{ok, Node} = mnesia:subscribe({table, schema, detailed}),
	Db = ets:new(?TABLE, [bag, named_table]),
	Tables = lists:foldl(fun
		(schema, Acc) -> Acc;
		(T, Acc) -> 
			Attrs = mnesia:table_info(T, all),
			case should_track(Attrs) of 
				true -> 
					mnesia:subscribe({table, T, detailed}),
					sets:add_element(T, Acc);
				false -> 
					error_logger:info_msg("~p (init): NOT tracking table ~p~n",
						[?MODULE, T]),
					Acc
			end
		end, sets:new(), mnesia:system_info(tables)),
	Mode = case mnesia:system_info(db_nodes) -- mnesia:system_info(running_db_nodes) of 
		[] -> queue;
		_  -> store
	end,
	error_logger:info_msg("~p (init): starting in ~p mode, tracking ~p~n", 
		[?MODULE, Mode, sets:to_list(Tables)]),
	{ok, #state{db=Db, mode=Mode, queue=queue:new(), tables=Tables}}.

handle_call(_Any, _From, State) -> 
	{reply, {error, badcall}, State, ?DEQUEUE_TIMEOUT}.

handle_cast(_Any, State) -> 
	{noreply, State, ?DEQUEUE_TIMEOUT}.

handle_info({mnesia_table_event, {write, schema, {schema, schema, _Attrs}, _, 
	_ActId}}, State) ->
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_table_event, {write, schema, {schema, Table, Attrs}, _, 
	_ActId}}, State) ->
	case {should_track(Attrs), sets:is_element(Table, State#state.tables)} of 
		{true, true} -> 
			{noreply, State, ?DEQUEUE_TIMEOUT};
		{true, false} -> 
			error_logger:info_msg("~p: starting tracking ~p", [?MODULE, Table]),
			Ns = sets:add_element(Table, State#state.tables),
			{noreply, State#state{tables=Ns}, ?DEQUEUE_TIMEOUT};
		{false, true} -> 
			error_logger:info_msg("~p: stop tracking ~p", [?MODULE, Table]),
			Nq = case State#state.mode of 
				queue -> 
					unqueue(State#state.queue, Table);
				store -> 
					ets:delete(?TABLE, Table),
					State#state.queue
			end,
			Ns = sets:del_element(Table, State#state.tables),
			{noreply, State#state{queue=Nq, tables=Ns}, ?DEQUEUE_TIMEOUT};
		{false, false} -> 
			{noreply, State, ?DEQUEUE_TIMEOUT}
	end;
handle_info({mnesia_table_event, {delete, schema, {schema, Table, _Attrs}, _,
	_ActId}}, State) -> 
	case sets:is_element(Table, State#state.tables) of 
		true -> 
			error_logger:info_msg("~p: stop tracking ~p (deleted)", 
				[?MODULE, Table]),
			Nq = case State#state.mode of 
				queue -> 
					unqueue(State#state.queue, Table);
				store -> 
					ets:delete(?TABLE, Table),
					State#state.queue
			end,
			Ns = sets:del_element(Table, State#state.tables),
			{noreply, State#state{queue=Nq, tables=Ns}, ?DEQUEUE_TIMEOUT};
		false -> 
			{noreply, State, ?DEQUEUE_TIMEOUT}
	end;
handle_info({mnesia_table_event, {write, Table, Record, [], _ActId}}, State) ->
	Nq = case sets:is_element(Table, State#state.tables) of 
		false -> State#state.queue;
		true  -> 
			case State#state.mode of 
				store -> 
					?debug("reunion(store): storing {~p, ~p}~n", 
						[Table, element(2, Record)]),
					ets:insert(?TABLE, {Table, element(2, Record)});
				queue -> ok
			end,
			?debug("reunion(~p): queueing {~p, ~p}, expires: ~p~n", 
					[State#state.mode, Table, element(2, Record), expires()]),
			queue:in(#qentry{table=Table, key=element(2, Record), expires=expires()}, 
				State#state.queue)
	end,
	{noreply, State#state{queue=Nq}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_table_event, {write, _Table, _Record, _NonEmptyList, _Act}}, State) -> 
	% this is update of already existing key, may ignore
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_table_event, {delete, Table, {Table, Key}, _Value, _ActId}}, State) -> 
	Nq = case sets:is_element(Table, State#state.tables) of 
		true -> 
			ets:delete_object(?TABLE, {Table, Key}),
			unqueue(State#state.queue, Table, Key);
		false -> 
			State#state.queue
	end,
	{noreply, State#state{queue=Nq}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_table_event, {delete, Table, Record, _Old, _ActId}}, State) -> 
	Key = element(2, Record),
	Nq = case sets:is_element(Table, State#state.tables) of 
		true -> 
			ets:delete_object(?TABLE, {Table, Key}),
			unqueue(State#state.queue, Table, Key);
		false -> 
			State#state.queue
	end,
	{noreply, State#state{queue=Nq}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {mnesia_up, Node}}, State) when 
	State#state.mode == store -> 
	Mode = case nextmode() of 
		store -> store;
		queue -> 
			?debug("reunion(store->queue): removing all keys~n", []),
			ets:delete_all_objects(?TABLE),
			queue
	end,
	error_logger:info_msg("~p: got mnesia_up ~p in store mode, next mode: ~p", 
		[?MODULE, Node, Mode]),
	{noreply, State#state{mode=Mode}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {mnesia_down, Node}}, State) when 
	State#state.mode == queue -> 
	% mirror all queued entries to ets
	queue_mirror(State#state.queue),
	error_logger:info_msg("~p: got mnesia_down ~p in queue mode, switching to store", 
		[?MODULE, Node]),
	{noreply, State#state{mode=store}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {inconsistent_database, Context, Node}}, State) -> 
	error_logger:info_msg("~p: Inconsistency detected. Context = ~p, Node ~p~n",
		[?MODULE, Context, Node]),
	Res = global:trans({?LOCK, self()}, 
		fun() -> 
			error_logger:info_msg("~p: have global lock. mnesia locks: ~p", 
				[?MODULE, mnesia_locker:get_held_locks()]),
			error_logger:info_msg("~p: nodes: ~p,~n   running: ~p,~n"
				"   ~p messages: ~p~n", 
				[?MODULE, mnesia:system_info(db_nodes), 
					mnesia:system_info(running_db_nodes), 
					process_info(self(), message_queue_len), 
					process_info(self(), messages)]),
			stitch_together(Node)
		end),
	error_logger:info_msg("~p: stitching with ~p: ~p", [?MODULE, Node, Res]),
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info(timeout, State) -> 
	Now = os:timestamp(),
	Queue = dequeue(State#state.queue, Now),
	{noreply, State#state{queue=Queue}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event,{mnesia_info, _, _}}, State) -> 
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {mnesia_down, _Node}}, State) 
	when State#state.mode == store -> 
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info(Any, State) -> 
	error_logger:info_msg("~p: unhandled info ~p~n", [?MODULE, Any]),
	{noreply, State, ?DEQUEUE_TIMEOUT}.

terminate(Reason, _State) -> 
	error_logger:info_msg("~p: terminating (~p)", [?MODULE, Reason]),
	ok.

code_change(_Old, State, _Extra) -> 
	{ok, State}.

should_track(Attr) -> 
	LocalContent = proplists:get_value(local_content, Attr),
	Type = proplists:get_value(type, Attr),
	AllNodes = proplists:get_value(all_nodes, Attr),
	if 
		LocalContent == true -> 
			false;
		Type == bag ->  % sets and ordered_sets are ok
			false;
		length(AllNodes) == 1 ->
			false;
		true -> true
	end.

expires() -> 
	{M, S, Ms} = os:timestamp(), 
	{M, S+?EXPIRE_TIMEOUT, Ms}.

dequeue(Queue, Now) -> 
	case queue:out(Queue) of 
		{empty, Queue} -> Queue;
		{{value, #qentry{expires=Exp}}, _Q1} when Exp > Now -> 
			Queue;
		{{value, #qentry{} = _Qe}, Q1} -> 
			?debug("reunion(dequeue): expired ~p (~p)~n", [_Qe, Now]),
			dequeue(Q1, Now)
	end.

unqueue(Queue, Table, Key) -> 
	queue:filter(fun
		(#qentry{table=T, key=K} = _Qe) when T==Table, K==Key -> 
			?debug("reunion(unqueue/3): deleted ~p~n", [_Qe]),
			false;
		(#qentry{}) -> 
			true
		end, Queue).

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
		{{value, #qentry{table=T, key=K}}, Q1} -> 
			?debug("reunion(queue_mirror): storing {~p, ~p}~n", [T, K]),
			ets:insert(?TABLE, {T, K}),
			queue_mirror(Q1)
	end.

stitch_together(Node) -> 
	case lists:member(Node, mnesia:system_info(running_db_nodes)) of 
		true -> 
			error_logger:info_msg("~p: node ~p already running, not "
				"stitching~n", [?MODULE, Node]),
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
					error_logger:info_msg("~p: MergeF ret = ~p", 
						[?MODULE, Res]),
					stitch_tabs(TabMethods, Node),
					Res;
				Other -> 
					error_logger:info_msg("~p: MergeF ret = ~p (nonstitch)", 
						[?MODULE, Other]),
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
	end.

run_stitch(#s0{module=M, function=F, table=Tab, remote=Remote, type=Type,
	modstate=MSt}) -> 
	LocalKeys = mnesia:dirty_all_keys(Tab),
	% usort used to remove duplicates
	Keys = lists:usort(lists:concat([LocalKeys, remote_keys(Remote, Tab)])),
	lists:foldl(fun(K, Sx) -> 
		A = mnesia:read({Tab, K}),
		B = remote_object(Remote, Tab, K), 
		case {A, B} of 
			{[], []} -> 
				% element is not present anymore
				Sx;
			{Aa, Aa} -> 
				% elements are the same
				Sx;
			{[Aa], []} when Type == set -> 
				% remote element is not present
				case is_locally_inserted(Tab, K) of 
					true -> 
						?debug("reunion(stitch ~p ~p ~p ~p): write remote~n", 
							[Tab, Type, A, B]),
						write(Remote, Aa);
					false -> 
						?debug("reunion(stitch ~p ~p ~p ~p): delete local~n", 
							[Tab, Type, A, B]),
						delete(Aa)
				end,
				Sx;
			{[], [Bb]} when Type == set -> 
				% local element not present
				case is_remotely_inserted(Tab, K, Remote) of 
					true -> 
						?debug("reunion(stitch ~p ~p ~p ~p): write local~n", 
							[Tab, Type, A, B]),
						write(Bb);
					false -> 
						?debug("reunion(stitch ~p ~p ~p ~p): delete remote~n", 
							[Tab, Type, A, B]),
						delete(Remote, Bb)
				end,
				Sx;
			{[Aa], [Bb]} when Type == set -> 
				Sn = case M:F(Aa, Bb, Sx) of 
					{ok, left, Sr} -> 
						?debug("reunion(stitch ~p ~p ~p ~p): left, write remote~n", 
							[Tab, Type, A, B]),
						write(Remote, Aa), 
						Sr;
					{ok, right, Sr} -> 
						?debug("reunion(stitch ~p ~p ~p ~p): right, write local~n", 
							[Tab, Type, A, B]),
						write(Bb), Sr;
					{ok, both, Sr} when Type == bag -> 
						?debug("reunion(stitch ~p ~p ~p ~p): both, write both~n", 
							[Tab, Type, A, B]),
						write(Bb),
						write(Remote, Aa), 
						Sr;
					{ok, neither, Sr} -> 
						?debug("reunion(stitch ~p ~p ~p ~p): neither, delete both~n", 
							[Tab, Type, A, B]),
						delete(Aa),
						delete(Remote, Bb), 
						Sr;
					{inconsistency, Error, Sr} -> 
						?debug("reunion(stitch ~p ~p ~p ~p): inconsistency ~p~n", 
							[Tab, Type, A, B, Error]),
						report_inconsistency(Tab, Aa, Bb, Error),
						Sr
				end,
				Sn;
			{[Aa], [Bb]} when Type == bag -> 
				Sn = case M:F(Aa, Bb, Sx) of 
					{ok, Actions, Sr} -> 
						do_actions(Actions, Remote),
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
do_actions([{delete_local, Ae}|Next], Remote) -> 
	delete(Ae), do_actions(Next, Remote);
do_actions([{write_remote, Ae}|Next], Remote) -> 
	write(Remote, Ae), do_actions(Next, Remote);
do_actions([{delete_remote, Ae}|Next], Remote) -> 
	delete(Remote, Ae), do_actions(Next, Remote).

affected_tables(IslandB) -> 
	IslandA = mnesia:system_info(running_db_nodes),
	Tables = mnesia:system_info(tables) -- [schema], 
	lists:foldl(fun(T, Acc) -> 
		Attrs = mnesia:table_info(T, all),
		Tracked = should_track(Attrs),
		Nodes = mnesia:table_info(T, all_nodes),
		case {intersection(IslandA, Nodes), intersection(IslandB, Nodes), 
			Tracked} of 
			{[_|_], [_|_], true} -> 
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
	ask_remote(Remote, {get_keys, Tab}).

remote_object(Remote, Tab, Key) -> 
	ask_remote(Remote, {get_object, Tab, Key}).

ask_remote(Remote, Query) -> 
	case rpc:call(Remote, ?MODULE, handle_remote, [Query, self()]) of 
		{badrpc, {'EXIT', {undef, [{?MODULE, handle_remote, _, _}, _]} -> 
			error_logger:error_msg("~p: unable to query ~p for "
				"handle_remote(~p): ~p", [?MODULE, Remote, Query, Reason]),
			error(badrpc);
		Other -> 
			Other
	end.

handle_remote({write, A}, _) -> 
	write(A);
handle_remote({delete, A}, _) -> 
	delete(A);
handle_remote({get_keys, Tab}, Pid) ->  % XXXXX - implement Pid tracking
	mnesia:dirty_all_keys(Tab);
handle_remote({get_object, Tab, Key}, _Pid) -> 
	mnesia:dirty_read({Tab, Key});
handle_remote({is_locally_inserted, Tab, Key}, _Pid) -> 
	is_locally_inserted(Tab, Key).

is_locally_inserted(Tab, Key) -> 
	case ets:lookup(?TABLE, Tab) of 
		[] -> 
			?debug("reunion(is_locally_inserted): {~p, ~p} is not~n", [Tab, Key]),
			false;
		List -> 
			?debug("reunion(is_locally_inserted): {~p, ~p} in ~p: ~p~n", 
				[Tab, Key, List, lists:keymember(Key, 2, List)]),
			lists:keymember(Key, 2, List)
	end.

is_remotely_inserted(Tab, Key, Node) -> 
	ask_remote(Node, {is_locally_inserted, Tab, Key}).

report_inconsistency(Tab, A, B, Error) -> 
	alarm_handler:set_alarm({{reunion, inconsistency, [Tab, A, B]}, Error}).

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
