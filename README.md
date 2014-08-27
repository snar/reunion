
reunion
=======

This is another approach to make `mnesia` more partition-tolerant. 
Results are not 100% accurate (see below for known problems), but it's much 
better than default "do not even try to recover after split".

Usage:
------

	(node@host)>application:start(reunion).

This will start `reunion` in default configuraion, tracking all keys in
`set` and `ordered_set` tables with default strategy of `merge_only` and 
trying to reconnect all not running nodes every `net_tick_timeout` seconds.

Advanced usage:
---------------

`reunion` tries to detect two types of collisions: 
- INSERT/DELETE collision, when some record is present on one node and 
not present on another.
- Data Collision, when some record present on both nodes but contents differ.

In order to resolve first type of collision for `set` tables, `reunion` tracks
insert and delete operations, storing info about Table, Key, Operation and 
WhenItHappened in internal `ets` table. When collision occurs, `reunion` 
consults this table trying to determine last event for this key and restores 
record accordingly (if last found event is 'insert' - record is re-inserted 
on node it missed, if 'delete' - record is deleted on node it still present).

For `bag` tables operations are not tracked, and default behaviour is just to 
merge bags, adding missing elements on node they are not present. 

With Data Collision, when elements are present on both nodes with different
content, conflict resolution function is called. Default behaviour for 
`set` tables is to do nothing but to raise alarm (using `sasl` `alarm_handler`),
and for `bag` tables both elements are written on both nodes.

There are two more pre-defined strategies: `last_version`, that selects
"best" record using comparision on some field in the record, 

	(node@host)>mnesia:write_table_property(kvs, {reunion_compare, 
		{reunion_lib, last_version, [field]}}).

and `last_modified` (variant of last_version using pre-defined `modified` field
for comparison):

	(node@host)>mnesia:write_table_property(kvs, {reunion_compare, 
		{reunion_lib, last_modified, []}}).

These strategies can be used for `bag` tables too, but configuration is a bit
different: to select "best" element among a bag, elements should have some
"secondary key" field, and "best" element is selected only among elements with
the same "secondary key".

You can define your own conflict resolution functon, which will be called as:

	function(init, {Table :: atom(), Type :: 'set' | 'bag', Fields :: list(atom()), 
		RemoteNode :: atom()) -> 
		{ok, Modstate :: any()};
	function(done, Modstate :: any(), RemoteNode :: atom()) -> 
		any();
	function(LocalRecords, RemoteRecord, ModState :: any()) -> 
		{ok, Actions :: reunion:action() | list(reunion:action()), 
			NextState :: any()} | 
		{inconsistency, Error, NextState :: any()}
		
where `Action` can be one of 

	{write_local, Record} | {write_remote, Record} | {delete_local, Record} | 
		{delete_remote, Record}

I hope, the names are self-descriptive enough.

Excluding table from automatic merging:
---------------------------------------

	(node@host)>mnesia:write_table_property(bag, {reunion_compare, ignore}).

Disabling automatic reconnects: 
-------------------------------
	
	(node@host)>application:set_env(reunion, reconnect, never).

With this setting no new reconnect timers will be scheduled. If you want
to 

Known problems: 
---------------

As the comparison is done key-by-key, it can be slooow, especially over WAN links.
Simple math: if you need to check 10.000 values over link with 10ms latency it 
just can't take less than 100 seconds... 

Error window: it's possible that some conflicts will not be found or that 
reunion can introduce `missing update` problems. This is caused by the 
fact that no table locking used, so objects in mnesia can be changed 
after object is fetched for compare but before resulting object is 
written or before mnesia nodes are joined again.

Another interesting edge case: let's assume netsplit happens right after
creating some record and then record is deleted during netsplit. This can
be detected and resolved correctly only in case `reuinion` is running on 
both nodes and clocks are synchronised. If there are no `reunion` on 
island where record is deleted - this deletion will be missed.

Other approaches:
-----------------

`reunion` is an "almost complete rewrite" of `unsplit` by Ulf Wiger. 
Major difference between our approaches: `unsplit` uses a stateless 
approach and just unable to resolve "object present here and not present 
there" case: this can be a result of local insertion or remote deletion, 
and it's not possible to determine what happened without storing this information.

Thanks: 
-------

Ulf Wiger for his `unsplit` application. 

