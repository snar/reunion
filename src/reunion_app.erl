-module(reunion_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([start/0]).

start() -> 
	application:ensure_all_started(sasl),
	application:ensure_all_started(mnesia),
	application:start(reunion).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
	reunion_sup:start_link().

stop(_State) ->
	ok.

