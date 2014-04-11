-module(reunion).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
	code_change/3]).
-export([start_link/0]).
-export([handle_remote/2, is_locally_inserted/2, is_remotely_inserted/3]).
-export([report_inconsistency/3]).

-define(TABLE, ?MODULE).
-define(DEQUEUE_TIMEOUT, 1000).
-define(EXPIRE_TIMEOUT, 60). % seconds
-define(LOCK, {?MODULE, lock}).
-define(DEFAULT_METHOD, {reunion_lib, merge_only, []}).
-define(DONE, {?MODULE, merge_done}).

-record(qentry, {table, key, expires}).
-record(state, {db, queue, mode, tables = sets:new()}).
-record(s0, {table, attributes, module, function, xargs, remote, modstate}).

start_link() -> 
	gen_server:start_link({local, ?MODULE}, ?MODULE, [[]], []).

init(_Args) -> 
	mnesia:subscribe(system),
	Db = ets:new(?TABLE, [bag, named_table]),
	Tables = lists:foldl(fun
		(T, Acc) -> 
			case is_table_tracked(T) of 
				true -> 
					mnesia:subscribe({table, T, detailed}),
					sets:add_element(T, Acc);
				false -> 
					error_logger:info_msg("~p (init): NOT tracking table ~p~n", [?MODULE, T]),
					Acc
			end
		end, sets:new(), mnesia:system_info(tables)),
	Mode = case mnesia:system_info(db_nodes) -- mnesia:system_info(running_db_nodes) of 
		[] -> queue;
		_  -> store
	end,
	error_logger:info_msg("~p (init): starting in ~p mode, tracking ~p~n", [?MODULE, Mode, sets:to_list(Tables)]),
	{ok, #state{db=Db, mode=Mode, queue=queue:new(), tables=Tables}}.

handle_call(_Any, _From, State) -> 
	{reply, {error, badcall}, State, ?DEQUEUE_TIMEOUT}.

handle_cast(_Any, State) -> 
	{noreply, State, ?DEQUEUE_TIMEOUT}.

handle_info({mnesia_table_event, {write, Table, Record, [], _ActId}}, State) when State#state.mode == queue -> 
	Nq = case sets:is_element(Table, State#state.tables) of 
		false -> State#state.queue;
		true  -> 
			queue:in(#qentry{table=Table, key=element(2, Record), expires=expires()}, State#state.queue)
	end,
	{noreply, State#state{queue=Nq}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_table_event, {write, Table, Record, [], _ActId}}, State) when State#state.mode == store -> 
	Nq = case sets:is_element(Table, State#state.tables) of 
		false -> State#state.queue;
		true  -> 
			queue:in(#qentry{table=Table, key=element(2, Record), expires=expires()}, State#state.queue)
	end,
	ets:insert(?TABLE, {Table, element(2, Record)}),
	{noreply, State#state{queue=Nq}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_table_event, {write, _Table, _Record, _NonEmptyList, _Act}}, State) -> 
	% this is update of already existing key, may ignore
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_table_event, {delete, Table, {Table, Key}, _Value, _ActId}}, State) -> 
	Nq = case sets:is_element(Table, State#state.tables) of 
		true -> unqueue(State#state.queue, Table, Key);
		false -> State#state.queue
	end,
	ets:delete_object(?TABLE, {Table, Key}),
	{noreply, State#state{queue=Nq}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {mnesia_up, Node}}, State) when State#state.mode == store -> 
	Mode = case nextmode() of 
		store -> store;
		queue -> 
			ets:delete_all_objects(?TABLE),
			queue
	end,
	error_logger:info_msg("~p: got mnesia_up ~p in store mode, next mode: ~p", [?MODULE, Node, Mode]),
	{noreply, State#state{mode=Mode}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {mnesia_down, Node}}, State) when State#state.mode == queue -> 
	% mirror all queued entries to ets
	queue_mirror(State#state.queue),
	error_logger:info_msg("~p: got mnesia_down ~p in queue mode, switching to store", [?MODULE, Node]),
	{noreply, State#state{mode=store}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {inconsistent_database, Context, Node}}, State) -> 
	error_logger:info_msg("~p: Inconsistency detected. Context = ~p, Node ~p~n",
		[?MODULE, Context, Node]),
	Res = global:trans({?LOCK, self()}, 
		fun() -> 
			error_logger:info_msg("~p: have global lock. mnesia locks: ~p", 
				[?MODULE, mnesia_locker:get_held_locks()]),
			error_logger:info_msg("~p: nodes: ~p, running: ~p, ~p messages: ~p", 
				[?MODULE, mnesia:system_info(db_nodes), mnesia:system_info(running_db_nodes),
				process_info(self(), message_queue_len), process_info(self(), messages)]),
			stitch_together(Node)
		end),
	error_logger:info_msg("~p: stitching with ~p: ~p", [?MODULE, Node, Res]),
	error_logger:info_msg("~p: locks after stitch: ~p", [?MODULE, mnesia_locker:get_held_locks()]),
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info(timeout, State) -> 
	Now = os:timestamp(),
	Queue = dequeue(State#state.queue, Now),
	{noreply, State#state{queue=Queue}, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event,{mnesia_info, _, _}}, State) -> 
	{noreply, State, ?DEQUEUE_TIMEOUT};
handle_info({mnesia_system_event, {mnesia_down, Node}}, State) 
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

is_table_tracked(schema) -> false;
is_table_tracked(T) -> 
	case {mnesia:table_info(T, local_content), mnesia:table_info(T, type)} of 
		{false, set} -> true;
		{_, _} -> false
	end.

expires() -> 
	{M, S, Ms} = os:timestamp(), 
	{M, S+?EXPIRE_TIMEOUT, Ms}.

dequeue(Queue, Now) -> 
	case queue:out(Queue) of 
		{empty, Queue} -> Queue;
		{{value, #qentry{expires=Exp} = Qe}, _Q1} when Exp < Now -> 
			queue:in_r(Qe, Queue);
		{{value, #qentry{}}, Q1} -> 
			dequeue(Q1, Now)
	end.

unqueue(Queue, Table, Key) -> 
	queue:filter(fun
		(#qentry{table=T, key=K}) when T==Table, K==Key -> 
			false;
		(#qentry{}) -> 
			true
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
			ets:insert(?TABLE, {T, K}),
			queue_mirror(Q1)
	end.

stitch_together(Node) -> 
	case lists:member(Node, mnesia:system_info(running_db_nodes)) of 
		true -> 
			error_logger:info_msg("~p: node ~p already running, not stitching~n", [?MODULE, Node]),
			ok;
		false -> 
			pre_stitch_together(Node)
	end.

pre_stitch_together(Node) -> 
	case rpc:call(Node, mnesia, system_info, [is_running]) of 
		yes -> 
			do_stitch_together(Node);
		Other -> 
			error_logger:info_msg("~p: node ~p: mnesia not running (~p), not stitching~n", [?MODULE, Node, Other]),
			ok
	end.
			
do_stitch_together(Node) -> 
	IslandB = case rpc:call(Node, mnesia, system_info, [running_db_nodes]) of 
		{badrpc, Reason} -> 
			error_logger:info_msg("~p: unable to ask mnesia:system_info(running_db_nodes) on ~p: ~p", [?MODULE, Node, Reason]),
			error(badrpc);
		Answer -> 
			Answer
	end,
	TabsAndNodes = affected_tables(IslandB),
	Tables = [ T || {T, _} <- TabsAndNodes ],
	DefaultMethod = default_method(),
	TabMethods = [{T, Ns, get_method(T, DefaultMethod)} || {T, Ns} <- TabsAndNodes],
	error_logger:info_msg("~p: calling connect_nodes(~p) with Tables = ~p", [?MODULE, Node, Tables]),
	mnesia_controller:connect_nodes([Node], 
		fun(MergeF) -> 
			case MergeF(Tables) of 
				{merged, _, _} = Res -> 
					error_logger:info_msg("~p: MergeF ret = ~p", [?MODULE, Res]),
					stitch_tabs(TabMethods, Node),
					Res;
				Other -> 
					error_logger:info_msg("~p: MergeF ret = ~p (nonstitch)", [?MODULE, Other]),
					Other
			end
		end).

stitch_tabs(TabMethods, Node) -> 
	[ do_stitch(TM, Node) || TM <- TabMethods ].

do_stitch({Tab, _Nodes, {M, F, Xargs}}, Node) -> 
	Attrs = mnesia:table_info(Tab, attributes),
	S0 = #s0{module = M, function=F, xargs=Xargs, table=Tab, 
		attributes = Attrs, remote = Node},
	try run_stitch(check_return(M:F(init, [Tab, Attrs|Xargs], Node), S0)) of 
		ok -> ok;
		_  -> error(badreturn)
	catch 
		throw:?DONE -> ok
	end.

check_return(stop, _S0) -> 
	throw(?DONE);
check_return({ok, St}, S) -> 
	S#s0{modstate=St};
check_return({ok, Actions, St}, S) -> 
	S1 = S#s0{modstate=St},
	perform_actions(Actions, S1).

run_stitch(#s0{module=M, function=F, table=Tab, remote=Remote, 
	modstate=MSt} = S) -> 
	LocalKeys = mnesia:dirty_all_keys(Tab),
	Keys = lists:concat([LocalKeys, remote_keys(Remote, Tab)]),
	lists:foldl(fun(K, Sx) -> 
		A = mnesia:read({Tab, K}),
		B = remote_object(Remote, Tab, K), 
		if 
			A == B -> Sx;
			true   -> 
				check_return(M:F([{A, B}], MSt, Remote), Sx)
		end
	end, S, Keys),
	M:F(done, MSt). 

affected_tables(IslandB) -> 
	IslandA = mnesia:system_info(running_db_nodes),
	Tables = mnesia:system_info(tables) -- [schema], 
	lists:foldl(fun(T, Acc) -> 
		Tracked = is_table_tracked(T),
		Nodes = lists:concat([mnesia:table_info(T, C) || C <- backend_types()]),
		case {intersection(IslandA, Nodes), intersection(IslandB, Nodes), Tracked} of 
			{[_|_], [_|_], true} -> 
				[{T, Nodes}|Acc];
			_ -> Acc
		end end, [], Tables).

perform_actions(Actions, S0) when is_tuple(Actions) -> 
	perform_actions([Actions], S0);
perform_actions(Actions, #s0{table=Tab, remote=R} = S0) -> 
	local_perform_actions(Tab, Actions),
	ask_remote(R, {actions, Tab, Actions}),
	S0.

local_perform_actions(Tab, Actions) -> 
	error_logger:info_msg("~p: handling action ~p on ~p", [?MODULE, Actions, Tab]),
	lists:foreach(fun
		({write, Data}) when is_list(Data) -> 
			[mnesia:dirty_write(Tab, D) || D <- Data];
		({write, Data}) -> 
			mnesia:dirty_write(Tab, Data);
		({delete, Data}) when is_list(Data) -> 
			[mnesia:dirty_delete({Tab, D}) || D <- Data];
		({delete, Data}) -> 
			mnesia:dirty_delete(Data)
		end, Actions).


remote_keys(Remote, Tab) -> 
	ask_remote(Remote, {get_keys, Tab}).

remote_object(Remote, Tab, Key) -> 
	ask_remote(Remote, {get_object, Tab, Key}).

ask_remote(Remote, Query) -> 
	case rpc:call(Remote, ?MODULE, handle_remote, [Query, self()]) of 
		{badrpc, Reason} -> 
			error_logger:error_msg("~p: unable to query ~p for handle_remote(~p): ~p", 
				[?MODULE, Remote, Query, Reason]),
			error(badrpc);
		Other -> 
			Other
	end.

handle_remote({actions, Tab, Actions}, _) -> 
	local_perform_actions(Tab, Actions);
handle_remote({get_keys, Tab}, Pid) ->  % XXXXX - implement Pid tracking
	ets:lookup(?TABLE, Tab);
handle_remote({get_object, Tab, Key}, _Pid) -> 
	mnesia:dirty_read({Tab, Key});
handle_remote({is_locally_inserted, Tab, Key}, _Pid) -> 
	is_locally_inserted(Tab, Key).

is_locally_inserted(Tab, Key) -> 
	case ets:lookup(?TABLE, Tab) of 
		[] -> false;
		List -> lists:keymember(Key, 2, List)
	end.

is_remotely_inserted(Tab, Key, Node) -> 
	ask_remote(Node, {is_locally_inserted, Tab, Key}).

report_inconsistency(Tab, A, B) -> 
	alarm_handler:set_alarm({reunion, inconsistency, [Tab, A, B]}).
	
backend_types() ->
	try mnesia:system_info(backend_types)
	catch
		exit:_ ->
			[ram_copies, disc_copies, disc_only_copies]
	end.

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
