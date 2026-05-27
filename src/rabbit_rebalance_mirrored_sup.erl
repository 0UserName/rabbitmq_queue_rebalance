-module(rabbit_rebalance_mirrored_sup).


-behaviour(mirrored_supervisor).


% Behaviour
-export([start_link/0, init/1]).


-rabbit_boot_step({rabbit_rebalance_mirrored_supervisor,
  [
    {description, "rebalance mirrored supervisor"},
    {mfa        , {rabbit_sup, start_supervisor_child, [?MODULE]}},
    {requires   , [kernel_ready]},
    {cleanup    , {rabbit_sup, stop_child, [?MODULE]}},
    {enables    , rabbit_rebalance_queue_decorator}
  ]}).


%%%===================================================================
%%% Behaviour implementation
%%%===================================================================


start_link() -> mirrored_supervisor:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).


%%%===================================================================


init([]) -> {ok,
  {
    {
      % strategy
      one_for_one,
      % intensity
      1,
      % period
      60
    },
    [
      {
        % id
        rabbit_rebalance_sup,
        % start
        {rabbit_rebalance_sup, start_link, []},
        % restart
        permanent,
        % shutdown
        infinity,
        % type
        supervisor,
        % modules
        [rabbit_rebalance_sup]
      }
    ]
  }
}.