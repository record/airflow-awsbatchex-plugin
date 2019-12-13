from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.plugins_manager import AirflowPlugin


class AWSBatchExOperator(AWSBatchOperator):
    template_fields = AWSBatchOperator.template_fields + ("parameters",)

    @apply_defaults
    def __init__(self, parameters=None):
        super().__init__(**kwargs)

        self.parameters = parameters

    def execute(self, context):
        self.log.info(
            'Running AWS Batch Job - Job definition: %s - on queue %s',
            self.job_definition, self.job_queue
        )
        self.log.info('AWSBatchOperator overrides: %s', self.overrides)

        self.client = self.hook.get_client_type(
            'batch',
            region_name=self.region_name
        )

        try:
            response = self.client.submit_job(
                jobName=self.job_name,
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                arrayProperties=self.array_properties,
                parameters=self.parameters,
                containerOverrides=self.overrides)

            self.log.info('AWS Batch Job started: %s', response)

            self.jobId = response['jobId']
            self.jobName = response['jobName']

            self._wait_for_task_ended()

            self._check_success_task()

            self.log.info('AWS Batch Job has been successfully executed: %s', response)
        except Exception as e:
            self.log.info('AWS Batch Job has failed executed')
            raise AirflowException(e)


class AWSBatchExPlugin(AirflowPlugin):
    name = "awsbatchex_plugin"
    hooks = []
    operators = [AWSBatchExOperator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
