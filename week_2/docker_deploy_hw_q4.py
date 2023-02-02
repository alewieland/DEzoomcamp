
# Week 2 | Homework
# from prefect orion
# import
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parametrized_flow import etl_parent_flow
from prefect.filesystems import GitHub
from prefect.blocks.notifications import SlackWebhook

# Set-up Infrastructure
github_block = GitHub.load("zoom")
slack_webhook_block = SlackWebhook.load("zoom")



# https://docs.prefect.io/api-ref/prefect/deployments/#prefect.deployments.Deployment.build_from_flow

gsc_git_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow-github",
    storage=github_block,
    entrypoint="week_2/parametrized_flow.py:etl_parent_flow"
)
slack_webhook_block.notify("Hello from alewieland!")

# execute
if __name__ == "__main__":
    gsc_git_dep.apply()
