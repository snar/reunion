
reunion
=======

This is another approach to make `mnesia` more partition-tolerant. 
Of course, according to 'CAP Theorem' this can make your data inconsistent.

Usage:
------

	(node@host)>application:start(reunion).

This will start `reunion` in default configuraion, tracking all keys in
`set` and `ordered_set` tables with default strategy of `merge_only`.

Advanced usage:
---------------

First, some words on "how it works": in the default operation mode (no network
splits present), `reunion` subscribes to all replicated `set` and `ordered_set` 
tables in system. When some record is inserted or removes, this change (table
and key only) is recorded and expired after 60 seconds (as my experiments shows,
it's enough for erlang to detect network splits). When network split happens,
all changes are moved to `ets` table and never auto-expired: so, you shall
be sure that your splits are not long enough to die from out-of-memory.

On recovery `mnesia` initiates system_event `inconsistent_database`, which
is trapped by `reunion` and then we try to recover system consistency. 
For each table and for each key in table we are checking if corresponding
values are present and equal on both nodes. If so, then we just moving to 
next key/next table, but if not...
For `set`s and `ordered_set`s: 
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

Of course, you are not limited to predefined strategies, and you can 
provide yours. Your resolution function must be arity-3 function and 
be ready to be called with: 

function(init, {Table, set | bag, Attributes, XAttributes}, Remote) -> 
	{ok, Modstate :: any()};
function(done, Modstate, Remote) -> 
	any();
function(A, B, Modstate :: any()) -> 
	{ok, Actions, NextState} | 
	{ok, left, NextState}    | 
	{ok, right, NextState}   | 
        {ok, both, NextState}    | 
	{inconsistency, Error, NextState}.

For bag tables behaviour is about the same. Major difference: bag keys are not tracked, 
so there is no easy way to decide if missing object was removed, so it's left to conflict 
resolution funcions. Default `merge_only` function makes no assumptions and just
adds missing objects to bags. 

There are also `last_version` and `last_modified` strategies predefined for bags too, 
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

Thanks: 
-------

Ulf Wiger for his `unsplit` application. 

