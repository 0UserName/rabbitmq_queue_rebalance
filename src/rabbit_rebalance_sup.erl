-module(rabbit_rebalance_sup).


-behaviour(supervisor).


%%%===================================================================
%%% Export
%%%===================================================================


-export([start_link/0, init/1]).


-export([start_rebalance_worker/2]).


%%%===================================================================
%%% Boot
%%%===================================================================


-rabbit_boot_step({rabbit_rebalance_sup,
  [
    {mfa     , {rabbit_sup, start_supervisor_child, [?MODULE]}},
    {requires, [kernel_ready]},
    {cleanup , {rabbit_sup, stop_child, [?MODULE]}},
    {enables , rabbit_rebalance_queue_decorator}
  ]
}).


%%%===================================================================
%%% Internal
%%%===================================================================


%% @private

get_rebalance_worker_spec(Id, Rebalance) ->
  #{
    id      => Id,
    start   => {rabbit_rebalance_worker, start_link, [Rebalance]},
    restart => transient,
    type    => worker,
    modules => [rabbit_rebalance_worker]
  }.


%% @private

pid_is(Id, Predicate) -> lists:any(fun ({_Id, Child, _, _}) -> _Id =:= Id andalso Predicate(Child) end, supervisor:which_children(?MODULE)).


%%%===================================================================
%%% Interface
%%%===================================================================


start_rebalance_worker(Id, Rebalance) -> case pid_is(Id, fun(Pid) -> Pid =:= undefined end) of true -> supervisor:restart_child(?MODULE, Id); false -> supervisor:start_child(?MODULE, get_rebalance_worker_spec(Id, Rebalance)) end.


%%%===================================================================
%%% Behaviour
%%%===================================================================


start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%%===================================================================


init([]) -> {ok,
  {
    #{
      strategy  => one_for_one,
      intensity => 1,
      period    => 15
    },
    [
      #{
        id      => rabbit_rebalance_monitor_worker,
        start   => {rabbit_rebalance_monitor_worker, start_link, []},
        restart => permanent,
        type    => worker,
        modules => [rabbit_rebalance_monitor_worker]
      }
    ]
  }
}.