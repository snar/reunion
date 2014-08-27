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
			fun last_version_bag/0,
			fun last_modified_bag/0,
			fun distributed_set/0
		]
	}.

start() -> 
	application:ensure_all_started(mnesia),
	application:ensure_all_started(reunion),
	{atomic, ok} = mnesia:create_table(reunion_test_set, [{attributes, [key, value]},
		{ram_copies, [node()|nodes()]}]),
	{atomic, ok} = mnesia:create_table(reunion_test_bag, [{type, bag}, 
		{attributes, [key, value, modified]}, {ram_copies, [node()|nodes()]}]),
	ok = mnesia:wait_for_tables([reunion_test_set, reunion_test_bag], 1000).

stop() -> 
	{atomic, ok} = mnesia:delete_table(reunion_test_set),
	{atomic, ok} = mnesia:delete_table(reunion_test_bag).
			

merge_only() -> 
	Ms1 =  reunion_lib:merge_only(init, {set, set, 
		mnesia:table_info(reunion_test_set, attributes), []}, node),
	{ok, set} = Ms1,
	Ms2 =  reunion_lib:merge_only([{set, a, b}], [{set, b, a}], set),
	{inconsistency, {merge, {set, a, b}, {set, b, a}}, set} = Ms2,
	ok  =  reunion_lib:merge_only(done, set, node),

	{ok, bag} = reunion_lib:merge_only(init, {reunion_test_bag, bag, 
		mnesia:table_info(reunion_test_set, attributes), []}, node),

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
	{ok, {write_local, {tab, a, 2}}, {set, 3}} = 
		reunion_lib:last_version([{tab, a, 1}], [{tab, a, 2}], {set, 3}),
	{ok, {write_remote, {tab, a, 2}}, {set, 3}} = 
		reunion_lib:last_version([{tab, a, 2}], [{tab, a, 1}], {set, 3}),
	ok = reunion_lib:last_version(done, {set, 3}, node).

last_modified_set() -> 
	{ok, {set, 3}} = reunion_lib:last_modified(init, {table, set, 
		[key, modified], []}, node),
	{ok, {write_local, {tab, a, 2}}, {set, 3}} = 
		reunion_lib:last_modified([{tab, a, 1}], [{tab, a, 2}], {set, 3}), 
	{ok, {write_remote, {tab, a, 2}}, {set, 3}} = 
		reunion_lib:last_modified([{tab, a, 2}], [{tab, a, 1}], {set, 3}),
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

	{value, {Aal, {tab, key, askey, 0}}, NextAa} = lists:keytake(
		{tab, key, askey, 0}, 2, Actions),
	delete_local = Aal,
	{value, {Aal1, {tab, key, askey, 1}}, NextAa1} = lists:keytake(
		{tab, key, askey, 1}, 2, NextAa),
	write_local  = Aal1,

	{value, {Bar, {tab, key, bskey, 1}}, NextAb} = lists:keytake(
		{tab, key, bskey, 1}, 2, NextAa1),
	delete_remote = Bar,

	{value, {BBr, {tab, key, bskey, 2}}, NextAb1} = lists:keytake(
		{tab, key, bskey, 2}, 2, NextAb),
	write_remote = BBr,

	[] = NextAb1.

last_modified_bag() -> 
	{ok, {bag, 4, 3}} = reunion_lib:last_modified(init, {tab, bag, 
		[key, skey, modified], [skey]}, node),
	{ok, Actions, {bag, 4, 3}} = reunion_lib:last_modified(
		[{tab, key, askey, 0}, {tab, key, bskey, 2}, {tab, key, cskey, 0}],
		[{tab, key, askey, 1}, {tab, key, bskey, 1}, {tab, key, cskey, 0}],
		{bag, 4, 3}),
	% in this simulation we should get following results: 
	%  - askey, 0, should be removed locally and replaced with askey, 1
	%  - bskey, 1, should be removed remotely and replaced with bskey, 2
        %_ - there should be no changes against cskey

	{value, {Aal, {tab, key, askey, 0}}, NextAa} = lists:keytake(
		{tab, key, askey, 0}, 2, Actions),
	delete_local = Aal,
	{value, {Aal1, {tab, key, askey, 1}}, NextAa1} = lists:keytake(
		{tab, key, askey, 1}, 2, NextAa),
	write_local  = Aal1,

	{value, {Bar, {tab, key, bskey, 1}}, NextAb} = lists:keytake(
		{tab, key, bskey, 1}, 2, NextAa1),
	delete_remote = Bar,

	{value, {BBr, {tab, key, bskey, 2}}, NextAb1} = lists:keytake(
		{tab, key, bskey, 2}, 2, NextAb),
	write_remote = BBr,

	[] = NextAb1.


distributed_set() -> 
	case nodes() -- [node()] of 
		[] -> 
			timer:sleep(1000), % to make sure that tables activated
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
			check_data(node()),
			Me ! {mnesia_up, node(), MyNode},
			ok
	after 100000 -> 
		ok
	end.
	
distributed_set(Node) -> 
	mnesia:subscribe(system),
	mnesia:dirty_write(#reunion_test_set{key=key1, value=value1}),
	mnesia:dirty_write(#reunion_test_set{key=key2, value=value2}),
	mnesia:dirty_write(#reunion_test_set{key=key5, value=value5}),

	Me = self(),
	MyNode = node(),

	timer:sleep(1000),

	erlang:spawn(Node, ?MODULE, distrib, [Me, MyNode]),
	ok = receive 
		running -> ok
	after 1000 -> 
		timeout
	end,

	net_kernel:disconnect(Node),

	ok = receive
		{mnesia_system_event, {mnesia_down, Node}} -> 
			ok
	after 1000 -> 
		timeout
	end,

	mnesia:dirty_write(#reunion_test_set{key=key3, value=value3}),
	mnesia:dirty_delete({reunion_test_set, key1}),

	timer:sleep(1000),

	reunion ! ping,

	receive 
		{mnesia_system_event, {mnesia_up, Node}} -> 
			check_data(Node)
	after 120000 -> 
		?assert(0)
	end,

	receive 
		{mnesia_up, Node, MyNode} -> ok
	after 10000 -> 
		?assert(0)
	end,
	Result = [{reunion_test_set,key2,value2}, {reunion_test_set,key3,value3}] ++ 
		case rpc:call(Node, erlang, whereis, [?MODULE]) of 
		undefined -> 
			% key5 was inserted and removed in too short time interval,
			% this can't be detected without remote helper. 
			[#reunion_test_set{key=key5, value=value5}];
		_ -> []
		end ++
 		[{reunion_test_set,rkey1,rvalue1}],
	Result = lists:sort(mnesia:dirty_match_object({reunion_test_set, '_', '_'})).
	
check_data(Remote) -> 
	[] = mnesia:dirty_read(reunion_test_set, key1),  % removed locally
	Expect = case rpc:call(Remote, erlang, whereis, [?MODULE]) of 
		undefined -> 
			[#reunion_test_set{key=key5, value=value5}];
		_ -> []
	end,
	Expect = mnesia:dirty_read(reunion_test_set, key5),  % removed remotely
	[#reunion_test_set{key=rkey1, value=rvalue1}] = 
		mnesia:dirty_read(reunion_test_set, rkey1),  % inserted remotely
	[#reunion_test_set{key=key3, value=value3}] = 
		mnesia:dirty_read(reunion_test_set, key3).  % inserted locally
