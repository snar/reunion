
reunion
=======

This is another approach to make `mnesia` more partition-tolerant. 
Not 100% accurate (see below for known problems), but it's much better
than default "do not even try to recover after split".

Usage:
------

	(node@host)>application:start(reunion).

This will start `reunion` in default configuraion, tracking all keys in
`set` and `ordered_set` tables with default strategy of `merge_only`.

Advanced usage:
---------------

First, some words on "how it works": in the default operation mode (no network
splits present), `reunion` subscribes to all replicated `set` and `ordered_set` 
tables in system. When some record is inserted or removed, this change (table,
key, operation and timestamp, not a full record) is recorded and expired after 
`net_kernel:get_net_ticktime()` seconds (maximum time to detect network split).

When network split happens, all changes are moved to `ets` table and never 
auto-expired: so, you shall be sure that your splits are not long enough to 
die from out-of-memory.

On recovery `mnesia` initiates system_event `inconsistent_database`, which
is trapped by `reunion` which tries to recover data consistency.

For each table and for each key in table it checks if corresponding
values are present and equal on both nodes. If so, then it just movies to 
next key/next table, but if not...

For `set`'s and `ordered_set`'s: 
- if we have an object on local node and do not have on remote: 
we are checking if there is a corresponding 'inserted' change on our node. 
If so, this difference was caused by local insert, and we push our object to 
remote node. Otherwise, this difference was caused by remote delete while 
partition, and so the local object can be removed.
- on the other hand, if there is no corresponding object on local node, but
there is one on remote: we checking, if there were corresponding 'delete'
change here. If so, object is deleted remotely, elsewhere remote object
is copied locally. 
- well, the most interesting thing happens when there are corresponding
objects on both nodes, but they are not equal, aka 'conflicting'. 
Of course, `reunion` have absolutely no idea on which object is better and
should be stored and which is worse and should be eliminated, so with the
default `merge_only` strategy it can't do nothing better than raise 
system alarm about this conflict. Different strategies are possible, 
there are two predefined ones: `last_modified` (which compares `modified`
attribute of your objects) and more generic `last_version`, which can 
compare any attribute. To configure use of a non-default strategy 
you should set `reunion_compare` attribute of this table to {M, F, A}:

	(node@host)>mnesia:write_table_property(kvs, {reunion_compare, 
		{reunion_lib, last_version, [value]}}).

or

	(node@host)>mnesia:write_table_property(kvs, {reunion_compare, 
		{reunion_lib, last_modified, []}}).

Of course, you are not limited to predefined strategies, and you can 
provide yours. Your resolution function must be arity-3 function and 
be ready to be called with: 

	function(init, {Table :: atom(), set | bag, Attributes :: list(atom()), 
		XAttributes :: list(any())}, Remote :: atom()) -> 
		{ok, Modstate :: any()};
	function(done, Modstate, Remote :: atom()) -> 
		any();
	function(A, B, Modstate :: any()) -> 
		{ok, Actions :: list(action()), NextState} | 
		{ok, left, NextState}    | 
		{ok, right, NextState}   | 
		{inconsistency, Error, NextState}
		when Action :: {write_local, Object} | 
			{write_remote, Object} | 
			{delete_local, Object} | 
			{delete_remote, Object}.

i hope the names are self-descriptive enough :)

For bag tables default behaviour is about the same. Major difference: bag keys are 
not tracked, so there is no easy way to decide if missing object was removed, so 
it's left to conflict resolution funcions. Default `merge_only` function makes no 
assumptions and just adds missing objects to bags. 

Also there are `last_version` and `last_modified` strategies predefined for bags too, 
but those require your objects to have some `secondary index` field, unique for objects 
with the same key, and comparison over `version`/`modified` field is done for objects
with the same `secondary key`. Of course, this means that you shall mention name of 
this secondary key as a second parameter in initialization:

	(node@host)>mnesia:write_table_property(bag, {reunion_compare, 
		{reunion_lib, last_version, [modified,value]}}).

Excluding table from automatic merging:
---------------------------------------

	(node@host)>mnesia:write_table_property(bag, {reunion_compare, ignore}).

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

