-module(rabbit_rebalance_queue_decorator).


-include_lib("../include/rabbit_rebalance.hrl").


-behaviour(rabbit_queue_decorator).


%%%===================================================================
%%% Export
%%%===================================================================


-export([startup/1, shutdown/1, policy_changed/2, active_for/1, consumer_state_changed/3]).


%%%===================================================================
%%% Boot
%%%===================================================================


-rabbit_boot_step({?MODULE,
  [
    {mfa     , {rabbit_queue_decorator, register, [<<"rebalance">>, ?MODULE]}},
    {requires, kernel_ready, rabbit_registry},
    {cleanup , {rabbit_queue_decorator, unregister, [<<"rebalance">>]}},
    {enables , recovery}
  ]
}).


%%%===================================================================
%%% Macro
%%%===================================================================


-define(RABBITMQ_REBALANCE_INTERVAL_ARG, <<"x-rebalance-interval">>).
-define(RABBITMQ_REBALANCE_EXCHANGE_ARG, <<"x-rebalance-exchange">>).


%%%===================================================================
%%% Internal
%%%===================================================================


%% @private

get_args(Q) ->
  Args = amqqueue:get_arguments(Q),
  {
    rabbit_misc:table_lookup(Args, ?RABBITMQ_REBALANCE_INTERVAL_ARG), %% {Type, Value} or undefined.
    rabbit_misc:table_lookup(Args, ?RABBITMQ_REBALANCE_EXCHANGE_ARG)  %% {Type, Value} or undefined.
  }.


%% @private

register(Q) -> case rabbit_amqqueue:consumers(Q) of [] -> rabbit_rebalance_monitor_worker:notify(register, #rebalance{resource = amqqueue:get_name(Q), last_consume = os:system_time(millisecond)}); _ -> ok end.


%%%===================================================================
%%% Behaviour
%%%===================================================================


%% @doc Callback invoked when the queue
%% becomes available following creation
%% or node restart.
%%
%% NOTICE: Called locally, i.e., on the
%% same node where the queue is located.
%%
%% Registers the queue in the monitor.

startup(Q) -> register(Q).


%%%===================================================================


%% @doc Callback invoked when the queue becomes unavailable following deletion or node shutdown.
%%
%% NOTICE: Called locally, i.e., on the
%% same node where the queue is located.

shutdown(Q) when ?is_amqqueue(Q) -> ok.


%%%===================================================================


%% @doc Callback invoked when
%% a policy is applied to the
%% queue.
%%
%% NOTICE: Called locally, i.e., on the
%% same node where the queue is located.

policy_changed(Q1, _Q2) when ?is_amqqueue(Q1) -> ok.


%%%===================================================================


%% @doc Callback invoked
%% during queue creation.
%%
%% NOTICE: Called locally, i.e., on the
%% same node where the queue is located.
%%
%% Verifies that
%% the queue has
%% the following
%% arguments:
%%
%% <ul>
%%     <li><code>x-rebalance-interval</code></li>
%%     <li><code>x-rebalance-exchange</code></li>
%% </ul>

active_for(Q) -> case get_args(Q) of {{long, _}, {longstr, _}} -> true; _ -> false end.


%%%===================================================================


%% @doc Callback invoked when the queue consumer state changed.
%%
%% NOTICE: Called locally, i.e., on the
%% same node where the queue is located.
%%
%% Registers the queue in the monitor.

consumer_state_changed(Q, _MaxActivePriority, _IsEmpty) -> register(Q).