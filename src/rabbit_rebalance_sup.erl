-module(rabbit_rebalance_sup).


-behaviour(supervisor).


% Behaviour
-export([start_link/0, init/1]).


% Interface
-export([is_running/1, start_rebalance_worker/1]).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private

get_monitor_worker_spec() ->
  #{
    id      => rabbit_rebalance_monitor_worker,
    start   => {rabbit_rebalance_monitor_worker, start_link, []},
    restart => permanent,
    type    => worker,
    modules => [rabbit_rebalance_monitor_worker]
  }.


%% @private

get_rebalance_worker_spec(Id) ->
  #{
    id      => Id,
    start   => {rabbit_rebalance_worker, start_link, [Id]},
    restart => transient,
    type    => worker,
    modules => [rabbit_rebalance_worker]
  }.


%%%===================================================================
%%% Interface
%%%===================================================================


%% @doc Checks if a process
%% with the specified Id is
%% running.

is_running(Id) -> lists:any(fun ({_Id, Child, _, _}) -> _Id =:= Id andalso Child =/= undefined end, supervisor:which_children(?MODULE)).


%%%===================================================================


%% @doc (Re)Starts a
%% process to handle
%% queue rebalancing.

start_rebalance_worker(Id) -> case supervisor:restart_child(?MODULE, Id) of {error,running} -> ok; _ -> supervisor:start_child(?MODULE, get_rebalance_worker_spec(Id)) end.


%%%===================================================================
%%% Behaviour implementation
%%%===================================================================


start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%%===================================================================


init([]) -> {ok, {#{strategy => one_for_one, intensity => 1, period => 15}, [get_monitor_worker_spec()]}}.