-module(rabbit_rebalance_queue_decorator).


-include_lib("rabbit/include/amqqueue.hrl").


-behaviour(rabbit_queue_decorator).


% Behaviour
-export([startup/1, shutdown/1, policy_changed/2, active_for/1, consumer_state_changed/3]).


-rabbit_boot_step({?MODULE,
  [
    {description, "rebalance queue decorator"},
    {mfa        , {rabbit_queue_decorator, register, [<<"rebalance">>, ?MODULE]}},
    {requires   , kernel_ready, rabbit_registry},
    {cleanup    , {rabbit_queue_decorator, unregister, [<<"rebalance">>]}},
    {enables    , recovery}
  ]}).


-define(RABBITMQ_REBALANCE_INTERVAL_ARG, <<"x-rebalance-interval">>).
-define(RABBITMQ_REBALANCE_EXCHANGE_ARG, <<"x-rebalance-exchange">>).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private

get_args(Q) ->
  Args = amqqueue:get_arguments(Q),

  {
    rabbit_misc:table_lookup(Args, ?RABBITMQ_REBALANCE_INTERVAL_ARG), %% {Type, Value} or undefined.
    rabbit_misc:table_lookup(Args, ?RABBITMQ_REBALANCE_EXCHANGE_ARG)  %% {Type, Value} or undefined.
  }.


%%%===================================================================
%%% Behaviour implementation
%%%===================================================================


startup(_Q) -> ok.


%%%===================================================================


%% @doc Callback invoked
%% during queue deletion.
%%
%% Deletes a queue
%% record from the
%% cache.

shutdown(Q) when ?is_amqqueue(Q) -> rabbit_rebalance_db:delete_queue_record(amqqueue:get_name(Q)),
  ok.


%%%===================================================================


policy_changed(Q1, _Q2) when ?is_amqqueue(Q1) -> ok.


%%%===================================================================


%% @doc Callback invoked
%% during queue creation.
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


%% @doc Callback invoked when the queue consumer state changes.
%%
%% Creates a queue record in the cache.

consumer_state_changed(Q, _MaxActivePriority, _IsEmpty) -> rabbit_rebalance_db:create_queue_record(amqqueue:get_name(Q), get_args(Q)),
  ok.