__author__ = 'rusty'


import boto3

if __name__ == '__main__':
    print('Connecting to SWF ..')
    conn = boto3.client('swf')

    while True:
        try:
            print('Polling for decision ..')
            task = conn.poll_for_decision_task(domain='SWF-Prototype', taskList={'name': 'PrintConsoleMessageTask'},
                                            identity='shaun-2')

            print('Decision: ', task)
            conn.respond_decision_task_completed(
                taskToken=task['taskToken'],
                decisions=[
                    {
                        'decisionType': 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes': {
                            'activityType':{
                                'name': 'PrintConsoleMessage',
                                'version': '1.0'
                                },
                            'activityId': 'fee78257-c71d-485f-b658-35f666bc20ad'
                        }
                    }
                ]
            )

            print(task)
        except Exception as e:
            print('Exception: ', e)
