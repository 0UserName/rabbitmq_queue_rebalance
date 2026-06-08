-module(rabbit_rebalance_maintenance_policy).


-include_lib("../include/rabbit_rebalance.hrl").


-behaviour(rabbit_policy_validator).


%%%===================================================================
%%% Export
%%%===================================================================


-export([validate_policy/1]).


%%%===================================================================
%%% Boot
%%%===================================================================


-rabbit_boot_step({?MODULE,
  [
    {mfa     , {rabbit_registry, register, [policy_validator, ?RABBITMQ_REBALANCE_MAINTENANCE_POLICY, ?MODULE]}},
    {requires, rabbit_registry},
    {enables , recovery}
  ]
}).


%%%===================================================================
%%% Internal
%%%===================================================================


%% @private

validate_policy(?RABBITMQ_REBALANCE_MAINTENANCE_POLICY, Value) when is_boolean(Value) -> ok;


%%%===================================================================


%% @private

validate_policy(?RABBITMQ_REBALANCE_MAINTENANCE_POLICY, Value) -> {error, "~tp must be a boolean, got ~tp", [?RABBITMQ_REBALANCE_MAINTENANCE_POLICY, Value]}.


%%%===================================================================
%%% Behaviour
%%%===================================================================


validate_policy(Terms) -> lists:foldl(fun ({Key, Value}, ok) -> validate_policy(Key, Value); (_, Error) -> Error end, ok, Terms).