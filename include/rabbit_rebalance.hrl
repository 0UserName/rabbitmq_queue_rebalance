-include_lib("rabbit/include/amqqueue.hrl").


%%%===================================================================
%%% Type
%%%===================================================================


-record(rebalance, {resource, args, last_consume, state = unknown, maintenance = false}).


%%%===================================================================
%%% Macro
%%%===================================================================


-define(RABBITMQ_REBALANCE_MAINTENANCE_POLICY, <<"x-rebalance-maintenance">>).