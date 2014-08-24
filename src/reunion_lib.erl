-module(reunion_lib).
-export([merge_only/3, last_version/3, last_modified/3]).

merge_only(init, {_,set,_,_}, _) -> {ok, set};
merge_only(done, _, _) -> ok;
merge_only(A, B, set) -> 
	{inconsistency, {merge, A, B}, set};
merge_only(A, B, bag) when is_list(A), is_list(B) -> 
	{Aonly, Bonly} = merge_bags(A, B),
	{ok, lists:append([lists:map(fun(Ae) -> {write_remote, Ae} end, Aonly),
		lists:map(fun(Be) -> {write_local, Be} end, Bonly)]), bag}. 

last_modified(init, {Table, set, Attrs, Xargs}, Node) -> 
	last_version(init, {Table, set, Attrs, [modified|Xargs]}, Node);
last_modified(A, B, C) -> 
	last_version(A, B, C).

last_version(init, {Table, set, Attrs, [VField|_]}, _Node) -> 
	{ok, {set, pos(VField, Table, Attrs)}};
last_version(init, {Table, bag, Attrs, [VField, IField|_]}, _Node) -> 
	{ok, {bag, pos(VField, Table, Attrs), pos(IField, Table, Attrs)}};
last_version(done, _State, _Node) -> 
	ok;
last_version(A, B, {set, Field}) when is_tuple(A), is_tuple(B) -> 
	case element(Field, A) >= element(Field, B) of 
		true -> {ok, left, Field};
		false -> {ok, right, Field}
	end;
last_version(A, B, {bag, Vfield, Ifield} = State) -> 
	Actions = merge_versioned(A, B, Vfield, Ifield, []),
	{ok, Actions, State}.
		
merge_bags(A, B) -> 
	merge_bags(lists:sort(A), lists:sort(B), [], []).

merge_bags(A, [], Aonly, Bonly) -> 
	{lists:append([A, Aonly]), Bonly};
merge_bags([], B, Aonly, Bonly) -> 
	{Aonly, lists:append([B, Bonly])};
merge_bags([A|NextA], [A|NextB], Aonly, Bonly) -> 
	merge_bags(NextA, NextB, Aonly, Bonly);
merge_bags([A|NextA], [B|NextB], Aonly, Bonly) -> 
	case A < B of 
		true -> 
			merge_bags(NextA, [B|NextB], [A|Aonly], Bonly);
		false -> 
			merge_bags([A|NextA], NextB, Aonly, [B|Bonly])
	end.

merge_versioned(A, [], _, _, Acts) -> 
	% local elements only
	lists:append([Acts, lists:map(fun(E) -> {write_remote, E} end, A)]);
merge_versioned([], B, _, _, Acts) -> 
	% remote elements only 
	lists:append([Acts, lists:map(fun(E) -> {write_local, E} end, B)]);
merge_versioned([A|NextA], B, VF, IF, Acts) -> 
	Index = element(IF, A),
	Avers = element(VF, A),
	case lists:keytake(Index, IF, B) of 
		false -> 
			% no corresponding element int set B at all, write remotely
			merge_versioned(NextA, B, VF, IF, [{write_remote, A}|Acts]);
		{value, Bb, NextB} -> 
			% there is a corresponding element. check version
			Bvers = element(VF, Bb),
			case Avers < Bvers of 
				true -> 
					% remote element has better version, update local
					merge_versioned(NextA, NextB, VF, IF,
						[{write_local, Bb}, {delete_local, A}|Acts]);
				false -> 
					% local element has better version, update remote side
					merge_versioned(NextA, NextB, VF, IF,
						[{write_remote, A}, {delete_remote, Bb}|Acts])
			end
	end.


pos(A, T, L) ->
	pos(A, T, L, 2).  % record tag is the 1st element in the tuple

pos(H, _, [H|_], P) ->
	P;
pos(H, Tab, [_|T], P) ->
	pos(H, Tab, T, P+1);
pos(A, Tab, [], _) ->
	mnesia:abort({missing_attribute, Tab, A}).

