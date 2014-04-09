-module(reunion_lib).
-export([merge_only/3, last_version/3, last_modified/3]).

merge_only(init, [Tab, _Attrs], _Remote) -> 
	{ok, {Tab}};
merge_only(done, _S, _Remote) -> 
	stop;
merge_only(Objs, {Tab} = S, Remote) -> 
	compare(Objs, Tab, 1, fun(_A, _B) -> neither end, S, Remote).
	

last_modified(init, S0, Remote) -> 
	last_version(init, S0 ++ [modified], Remote);
last_modified(Other, S, Remote) -> 
	last_version(Other, S, Remote).

last_version(init, [Tab, Attrs, Attr], _Remote) -> 
	case lists:member(Attr, Attrs) of 
		false -> 
			error_logger:info_msg("~p: no '~p' attribute in table ~p", 
				[?MODULE, Attr, Tab]),
			stop;
		true -> 
			Pos = pos(Attr, Tab, Attrs),
			error_logger:info_msg("~p: merging ~p on ~p (~p)~n",
				[?MODULE, Tab, Attr, Pos]),
			{ok, {Tab, Pos}}
	end;
last_version(done, _S, _Remote) -> 
	stop;
last_version(Objs, {T, P} = S, Remote) when is_list(Objs) -> 
	compare(Objs, T, P, fun(A, B) when A < B -> left;
						   (A, B) when A > B -> right;
						   (_, _) -> neither
						end, S, Remote).

compare(Objs, T, P, Comp, S, Remote) -> 
	case Objs of 
		[{[],  []}] -> {ok, S};
		[{[A], []}] -> 
			% this key presents only locally
			case reunion:is_locally_inserted(T, element(2, A)) of 
				true -> 
					{ok, {write, A}, S};
				false -> 
					{ok, {delete, A}, S}
			end;
		[{[], [B]}] -> 
			case reunion:is_remotely_inserted(T, element(2, B), Remote) of 
				true -> 
					{ok, {write, B}, S};
				false -> 
					{ok, {delete, B}, S}
			end;
		[{[A], [B]}] -> 
			ModA = element(P, A),
			ModB = element(P, B),
			case Comp(ModA, ModB) of 
				left -> {ok, {write, A}, S};
				right -> {ok, {write, B}, S};
				neither -> {error, {uncomparable, A, B}}
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


