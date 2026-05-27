PROJECT = rabbit_queue_rebalance
PROJECT_DESCRIPTION = RabbitMQ Rebalance

# Define dependencies on RabbitMQ core
DEPS = rabbit amqp_client

# Use the RabbitMQ plugin build system
DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk