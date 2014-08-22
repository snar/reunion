-module(reunion_lib).
-export([merge_only/3, last_version/3, last_modified/3]).

merge_only(init, {_,set,_,_}, _) -> {ok, set};
merge_only(stop, _, _) -> ok;
merge_only(A, B, set) -> 
	{inconsistency, {merge, A, B}, set}.

last_modified(init, {Table, set, Attrs, Xargs}, Node) -> 
	last_version(init, {Table, set, Attrs, [modified|Xargs]}, Node);
last_modified(A, B, C) -> 
	last_version(A, B, C).

last_version(init, {Table, set, Attrs, [VField|_]}, _Node) -> 
	{ok, {set, pos(VField, Table, Attrs)}};
last_version(stop, _State, _Node) -> 
	ok;
last_version(A, B, {set, Field}) when is_tuple(A), is_tuple(B) -> 
	case element(Field, A) >= element(Field, B) of 
		true -> {ok, left, Field};
		false -> {ok, rigth, Field}
	end.
		
pos(A, T, L) ->
	pos(A, T, L, 2).  % record tag is the 1st element in the tuple

pos(H, _, [H|_], P) ->
	P;
pos(H, Tab, [_|T], P) ->
	pos(H, Tab, T, P+1);
pos(A, Tab, [], _) ->
	mnesia:abort({missing_attribute, Tab, A}).

