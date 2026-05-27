-module(rabbit_rebalance_monitor_worker).


-include_lib("../include/rabbit_rebalance.hrl").


-behaviour(gen_server2).


% Behaviour
-export([start_link/0, code_change/3, handle_call/3, handle_cast/2, handle_continue/2, handle_info/2, init/1, terminate/2]).


-define(RABBITMQ_REBALANCE_CHECK_INTERVAL, 15000).


%%===================================================================
%%% Behaviour implementation
%%%===================================================================


start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%%%===================================================================


code_change(_OldVsn, State, _Extra) -> {ok, State}.


%%%===================================================================


handle_call(_Msg, _From, State) -> {reply, unknown_command, State}.


%%%===================================================================


handle_cast({check_lookup, #queue_record{resource = R} = Record}, State) -> case rabbit_amqqueue:lookup(R) of {ok, Q} -> {noreply, State, {continue, {check_state, Q, Record}}}; _ -> {noreply, State, {continue, {reconcile, R}}} end;


%%%===================================================================


handle_cast(_Msg, State) -> {noreply, State}.


%%%===================================================================


%% @doc Reschedules the timer
%% to re-trigger queue checks.

handle_continue(reschedule, State) -> erlang:send_after(?RABBITMQ_REBALANCE_CHECK_INTERVAL, self(), check),
  {noreply, State};


%%%===================================================================


handle_continue({reconcile, R}, State) -> rabbit_rebalance_db:delete_queue_record(R),
  {noreply, State};


%%%===================================================================


%% @doc Starts a process to
%% handle queue rebalancing.

handle_continue({rebalance, R}, State) -> rabbit_rebalance_sup:start_rebalance_worker(R),
  {noreply, State};


%%%===================================================================


handle_continue({check_alive, #queue_record{resource = R, last_consume = LC, interval = I}}, State) -> case os:system_time(millisecond) - LC < I of false -> {noreply, State, {continue, {rebalance, R}}}; _ -> {noreply, State} end;


%%%===================================================================


handle_continue({check_state, Q, Record}, State) -> case {amqqueue:get_state(Q), rabbit_amqqueue:consumers(Q)} of {live, []} -> {noreply, State, {continue, {check_alive, Record}}}; _ -> {noreply, State} end.


%%%===================================================================


%% @doc Triggers queue checks if the current node is available for
%% client service: a serving node is one where RabbitMQ is running
%% and not under maintenance.

handle_info(check, State) -> case rabbit:is_serving() of true -> maps:foreach(fun(_, Record) -> gen_server:cast(?MODULE, {check_lookup, Record}) end, rabbit_rebalance_db:select_queue_record()); _ -> ok end,
  {noreply, State, {continue, reschedule}};


%%%===================================================================


handle_info(_Msg, State) -> {noreply, State}.


%%%===================================================================


init(_Args) -> {ok, [], {continue, reschedule}}.


%%%===================================================================


terminate(_Reason, _State) -> ok.