-module(rabbit_rebalance_worker).


-include_lib("kernel/include/logger.hrl").

-include_lib("amqp_client/include/amqp_client.hrl").


-behaviour(gen_server2).


% Behaviour
-export([start_link/1, init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


-record(state, {conn, ch, resource, consumer_tag, timer}).


-define(CONNECTION_OPEN_TIMEOUT, 10000).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private

open_monitor(Params, ConnectionName) ->

  ?LOG_INFO("[~s] [~0p] Connecting to broker...", [?MODULE_STRING, self()]),

  case connect(Params, ConnectionName) of
    {ok, Conn, Ch} -> erlang:monitor(process, Ch),
      {ok, Conn, Ch};
    E              -> E
  end.


%%%===================================================================


%% @private
%%
%% @doc Opens AMQP connection and channel.

connect(Params, ConnectionName) ->
  try amqp_connection:start(Params, ConnectionName) of
    {ok, Conn} ->
      try amqp_connection:open_channel(Conn, {amqp_selective_consumer, []}) of
        {ok, Ch} ->
          {ok, Conn, Ch};
        E ->
          catch amqp_connection:close(Conn, ?CONNECTION_OPEN_TIMEOUT),
          E
      catch
        _:E ->
          catch amqp_connection:close(Conn, ?CONNECTION_OPEN_TIMEOUT),
          E
      end;
    E -> E
  catch
    _:E -> E
  end.


%%%===================================================================


%% @private
%%
%% @doc Unbind a queue from an exchange.

unbind_queue(#binding{source = #resource{name = S}, destination = #resource{name = D}, key = RK}, #state{ch = Ch}) ->

  ?LOG_INFO("[~s] [~0p] Unbinding the ~0p from the ~0p", [?MODULE_STRING, self(), S, D]),

  #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{exchange = S, queue = D, routing_key = RK}).


%%%===================================================================


%% @private
%%
%% @doc Start a queue consumer.
%%
%% This method asks the server to start a "consumer", which is a transient request for messages
%% from a specific queue. Consumers last as long as the channel they were declared on, or until
%% the client cancels them.

start_consumer(#state{ch = Ch, resource = #resource{name = QN}, consumer_tag = CT}) ->

  ?LOG_INFO("[~s] [~0p] Starting consumption from the ~0p", [?MODULE_STRING, self(), QN]),

  amqp_channel:cast(Ch, #'basic.consume'{queue = QN, consumer_tag = CT, exclusive = true}).


%%%===================================================================


%% @private
%%
%% @doc Publish a message.

publish_message(D, RK, Msg, #state{ch = Ch}) -> amqp_channel:call(Ch, #'basic.publish'{exchange = D, routing_key = RK}, Msg).


%%%===================================================================


%% @private
%%
%% @doc Acknowledge one message.

ack_message(DT, #state{ch = Ch}) -> amqp_channel:call(Ch, #'basic.ack'{delivery_tag = DT}).


%%%===================================================================


%% @private
%%
%% @doc End a queue consumer.

cancel_consumer(#state{ch = Ch, resource = #resource{name = QN}, consumer_tag = CT}) ->

  ?LOG_INFO("[~s] [~0p] Canceling consumption from the ~0p", [?MODULE_STRING, self(), QN]),

  amqp_channel:cast(Ch, #'basic.cancel'{consumer_tag = CT}).


%%%===================================================================


%% @private

try_cancel() -> gen_server:cast({local, ?MODULE}, try_cancel).


%%%===================================================================
%%% Behaviour implementation
%%%===================================================================


start_link(Args) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).


%%%===================================================================


init(#resource{} = R) -> {ok, R, {continue, init_connect}}.


%%%===================================================================


handle_continue(init_connect, #resource{virtual_host = VH} = R) ->

  Id = list_to_binary(?MODULE_STRING),

  case open_monitor(#amqp_params_direct{virtual_host = VH}, Id) of
    {ok, Conn, Ch} ->
      {noreply, #state{conn = Conn, ch = Ch, resource = R, consumer_tag = Id}, {continue, init_unbind}};
    Err ->
      {stop, Err, #state{}}
  end;


%%%===================================================================


handle_continue(init_unbind, State) ->

  % Skip default binding since it cannot be deleted.

  lists:foreach(fun(B) -> unbind_queue(B, State) end, lists:nthtail(1, rabbit_binding:list_for_destination(State#state.resource))),

  {noreply, State, {continue, init_consumer}};


%%%===================================================================


handle_continue(init_consumer, State) ->
  start_consumer(State),
  {ok, TRef} = timer:apply_after(15000, fun try_cancel/0),

  {noreply, State#state{timer = TRef}}.


%%%===================================================================


handle_call(_Msg, _From, State) -> {reply, unknown_command, State}.


%%%===================================================================


handle_cast(try_cancel, #state{resource = R} = State) ->

  {ok, Q} = rabbit_amqqueue:lookup(R),
  {ok, NumMsgs, _} = rabbit_amqqueue:stat(Q),

  ?LOG_INFO("[~s] [~0p] Number of messages: ~0p", [?MODULE_STRING, self(), NumMsgs]),

  case NumMsgs of
    0 ->
      cancel_consumer(State),
      {noreply, State#state{ch = undefined, consumer_tag = undefined}};
    _ ->
      {ok, TRef} = timer:apply_after(15000, fun try_cancel/0),
      {noreply, State#state{timer = TRef}}
  end;


%%%===================================================================


handle_cast(_Msg, State) -> {noreply, State}.


%%%===================================================================


%% @doc Confirm a new consumer.
%%
%% The server provides the client with a consumer tag, which is used
%% by the client for methods called on the consumer at a later stage.

handle_info(#'basic.consume_ok'{}, #state{resource = #resource{name = QN}} = State) ->

  ?LOG_INFO("[~s] [~0p] Consuming from the ~0p started", [?MODULE_STRING, self(), QN]),

  {noreply, State};


%%%===================================================================


%% @doc Notify the client of a consumer message.
%%
%% This method delivers a message to the client, via a consumer.

handle_info({#'basic.deliver'{delivery_tag = DT, exchange = E, routing_key = RK}, Msg}, State) ->
  publish_message(E, RK, Msg, State),
  ack_message(DT, State),

  {noreply, State};


%%%===================================================================


%% @doc Confirm a cancelled consumer.
%%
%% This method confirms that the cancellation was completed.

handle_info(#'basic.cancel_ok'{}, #state{resource = #resource{name = QN}} = State) ->

  ?LOG_INFO("[~s] [~0p] Consuming from the ~0p cancelled", [?MODULE_STRING, self(), QN]),

  {stop, {shutdown, 'rebalance.done'}, State#state{ch = undefined, consumer_tag = undefined}};


%%%===================================================================


handle_info({'DOWN', _Ref, process, _Pid, Reason}, State) ->

  ?LOG_INFO("[~s] [~0p] Channel is down due to reason: ~0p", [?MODULE_STRING, self(), Reason]),

  {stop, Reason, State};


%%%===================================================================


handle_info({'EXIT', _From, Reason}, State) ->

  ?LOG_INFO("[~s] [~0p] Channel is down due to reason: ~0p", [?MODULE_STRING, self(), Reason]),

  {stop, Reason, State}.


%%%===================================================================


terminate(Reason, #state{conn = Conn, resource = R, timer = TRef}) ->

  ?LOG_INFO("[~0p] [~0p] Terminate worker due to ~0p", [?MODULE_STRING, self(), Reason]),

  catch timer:cancel(TRef),
  case Reason of
    % Clean up state only if the
    % worker completes all steps;
    {_, 'rebalance.done'} ->
      rabbit_rebalance_db:delete_queue_record(R);
    _->
      ok
  end,
  catch amqp_connection:close(Conn),
  ok.


%%%===================================================================


code_change(_OldVsn, State, _Extra) -> {ok, State}.