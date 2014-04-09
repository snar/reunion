-module(reunion_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([start/0]).

start() -> 
	ensure_started(mnesia),
	ensure_started(lager),
	application:start(reunion).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
	reunion_sup:start_link().

stop(_State) ->
	ok.

ensure_started(App) -> 
	case application:start(App) of
		ok -> ok;
		{error, {already_started, App}} -> ok;
		{error, {not_started, Dep}} -> 
			ensure_started(Dep), ensure_started(App)
	end.
