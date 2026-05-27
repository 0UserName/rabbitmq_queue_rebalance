-module(rabbit_rebalance_db).


-include_lib("../include/rabbit_rebalance.hrl").

-include_lib("rabbit_common/include/resource.hrl").

-include_lib("khepri/include/khepri.hrl").


% Interface
-export([create_queue_record/2, select_queue_record/0, delete_queue_record/1]).


-define(RABBITMQ_KHEPRI_REBALANCE_PATH(VHost, Queue), [rabbitmq, rebalance, VHost, Queue]).


%%%===================================================================
%%% Interface
%%%===================================================================


%% @doc Creates a queue record in
%% the cache if it is not present.

create_queue_record(#resource{virtual_host = VH, name = QN} = R, {{_, I}, {_, E}}) -> rabbit_khepri:create(?RABBITMQ_KHEPRI_REBALANCE_PATH(VH, QN), #queue_record{resource = R, last_consume = os:system_time(millisecond), interval = I, exchange = E}).


%%%===================================================================


%% @doc Selects all queue records.

select_queue_record() -> case rabbit_khepri:get_many(?RABBITMQ_KHEPRI_REBALANCE_PATH(?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR)) of {ok, M} -> M; _ -> #{} end.


%%%===================================================================


%% @doc Deletes a queue record from the cache.

delete_queue_record(#resource{virtual_host = VH, name = QN}) -> rabbit_khepri:delete(?RABBITMQ_KHEPRI_REBALANCE_PATH(VH, QN)).