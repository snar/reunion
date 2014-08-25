-module(reunion_test).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-record(reunion_test_set, {key, value}).
-record(reunion_test_bag, {key, value, modified}).

reunion_test_() -> 
	{setup, spawn, 
		fun() -> start() end,
		fun(_) -> stop() end,
		[
			fun merge_only/0,
			fun last_version_set/0,
			fun last_modified_set/0,
			fun distributed_set/0
		]
	}.

start() -> 
	{atomic, ok} = mnesia:create_table(reunion_test_set, [{attributes, [key, value]},
		{ram_copies, nodes()}]),
	{atomic, ok} = mnesia:create_table(reunion_test_bag, [{type, bag}, 
		{attributes, [key, value, modified]}, {ram_copies, nodes()}]).

stop() -> 
	{atomic, ok} = mnesia:delete_table(reunion_test_set),
	{atomic, ok} = mnesia:delete_table(reunion_test_bag).
			

merge_only() -> 
	Node = hd(nodes() -- [node()]),
	Ms1 =  reunion_lib:merge_only(init, {set, set, 
		mnesia:table_info(reunion_test_set, attributes), []}, Node),
	{ok, set} = Ms1,
	Ms2 =  reunion_lib:merge_only({set, a, b}, {set, b, a}, set),
	{inconsistency, {merge, {set, a, b}, {set, b, a}}, set} = Ms2,
	ok  =  reunion_lib:merge_only(done, set, Node),

	{ok, bag} = reunion_lib:merge_only(init, {reunion_test_bag, bag, 
		mnesia:table_info(reunion_test_set, attributes), []}, Node),

	{ok, Actions, bag} = reunion_lib:merge_only([{a, a}, {a, b}], [{b, b}, {a, b}], 
		bag),
	{value, {ActA, {a, a}}, RestA} = lists:keytake({a, a}, 2, Actions),
	write_remote = ActA,
	{value, {ActB, {b, b}}, RestB} = lists:keytake({b, b}, 2, RestA),
	write_local = ActB,
	[] = RestB,
	ok.

last_version_set() -> 
	{ok, {set, 3}} = reunion_lib:last_version(init, {table, set, [key, ver], [ver]}, 
		node),
	{ok, right, {set, 3}} = reunion_lib:last_version({tab, a, 1}, {tab, a, 2}, 
		{set, 3}),
	{ok, left, {set, 3}} = reunion_lib:last_version({tab, a, 2}, {tab, a, 1}, {set, 3}),
	ok = reunion_lib:last_version(done, {set, 3}, node).

last_modified_set() -> 
	{ok, {set, 3}} = reunion_lib:last_modified(init, {table, set, [key, modified], []},
		node),
	{ok, right, {set, 3}} = reunion_lib:last_modified({tab, a, 1}, {tab, a, 2}, 
		{set, 3}), 
	{ok, left, {set, 3}} = reunion_lib:last_modified({tab, a, 2}, {tab, a, 1}, 
		{set, 3}),
	ok = reunion_lib:last_modified(done, {set, 3}, node).
	
last_version_bag() -> 
	{ok, {bag, 4, 3}} = reunion_lib:last_version(init, {tab, bag, [key, skey, version],
		[version, skey]}, node),
	{ok, Actions, {bag, 4, 3}} = reunion_lib:last_version(
		[{tab, key, askey, 0}, {tab, key, bskey, 2}, {tab, key, cskey, 0}],
		[{tab, key, askey, 1}, {tab, key, bskey, 1}, {tab, key, cskey, 0}],
		{bag, 4, 3}),
	% in this simulation we should get following results: 
	%  - askey, 0, should be removed locally and replaced with askey, 1
	%  - bskey, 1, should be removed remotely and replaced with bskey, 2
        %_ - there should be no changes against cskey

	{{value, {Aal, {tab, key, askey, 0}}}, NextAa} = lists:keytake(
		{tab, key, askey, 0}, 2, Actions),
	delete_local = Aal,
	{{value, {Aal1, {tab, key, askey, 1}}}, NextAa1} = lists:keytake(
		{tab, key, askey, 1}, 2, NextAa),
	write_local  = Aal1,

	{{value, {Bar, {tab, key, bskey, 1}}}, NextAb} = lists:keytake(
		{tab, key, bskey, 1}, 2, NextAa1),
	delete_remote = Bar,

	{{value, {BBr, {tab, key, bskey, 2}}}, NextAb1} = lists:keytake(
		{tab, key, bskey, 2}, 2, NextAb),
	write_remote = BBr,

	[] = NextAb1.


distributed_set() -> 
	case nodes() -- [node()] of 
		[] -> 
			ok;
		Nodes -> 
			distributed_set(hd(Nodes))
	end.

distrib(Me, MyNode) -> 
	mnesia:subscribe(system),
	Me ! running,
	receive 
		{mnesia_system_event, {mnesia_down, _}} -> 
			mnesia:dirty_write(#reunion_test_set{key=rkey1, value=rvalue1}),
			mnesia:dirty_delete({reunion_test_set, key5}),
			timer:sleep(1000),
			error_logger:info_msg("remote modifications done"),
			{reunion, MyNode} ! {remote_modifications, done}
	after 100000 -> 
		ok
	end,
	receive 
		{mnesia_system_event, {mnesia_up, MyNode}} -> 
			check_data(),
			Me ! {mnesia_up, node(), MyNode},
			ok
	after 100000 -> 
		ok
	end.
	
distributed_set(Node) -> 
	mnesia:dirty_write(#reunion_test_set{key=key1, value=value1}),
	mnesia:dirty_write(#reunion_test_set{key=key2, value=value2}),
	mnesia:dirty_write(#reunion_test_set{key=key5, value=value5}),

	mnesia:subscribe(system),
	Me = self(),
	MyNode = node(),

	Pid = erlang:spawn(Node, ?MODULE, distrib, [Me, MyNode]),
	ok = receive 
		running -> ok
	after 1000 -> 
		timeout
	end,

	net_kernel:disconnect(Node),

	mnesia:dirty_write(#reunion_test_set{key=key3, value=value3}),
	mnesia:dirty_delete({reunion_test_set, key1}),

	timer:sleep(1000),

	reunion ! ping,

	receive 
		{mnesia_system_event, {mnesia_up, Node}} -> 
			check_data()
	after 120000 -> 
		?assert(0)
	end,

	receive 
		{mnesia_up, Node, MyNode} -> ok
	after 10000 -> 
		?assert(0)
	end,
	[{reunion_test_set,key2,value2},
 		{reunion_test_set,key3,value3},
 		{reunion_test_set,rkey1,rvalue1}] = 
		lists:sort(mnesia:dirty_match_object({reunion_test_set, '_', '_'})).
	
check_data() -> 
	[] = mnesia:dirty_read(reunion_test_set, key1),  % removed locally
	[] = mnesia:dirty_read(reunion_test_set, key5),  % removed remotely
	[#reunion_test_set{key=rkey1, value=rvalue1}] = 
		mnesia:dirty_read(reunion_test_set, rkey1),  % inserted remotely
	[#reunion_test_set{key=key3, value=value3}] = 
		mnesia:dirty_read(reunion_test_set, key3).  % inserted locally
