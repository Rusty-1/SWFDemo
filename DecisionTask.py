__author__ = 'rusty'


import boto3

if __name__ == '__main__':
    print('Connecting to SWF ..')
    conn = boto3.client('swf')

    while True:
        try:
            print('Polling for decision ..')
            response = conn.poll_for_decision_task(domain='SWF-Prototype', taskList={'name': 'PrintConsoleMessageTask'},
                                            identity='merada-1')

            decisions = []
            last_event = response['events'][-1]

            print("+++ LAST EVENT +++" + last_event['eventType'])
            if last_event['eventType'] == 'DecisionTaskStarted':
                d = {'decisionType': 'ScheduleActivityTask',
                    'scheduleActivityTaskDecisionAttributes': {
                        'activityType': {
                            'name': 'PrintConsoleMessage',
                            'version': '1.1'
                            },
                        'activityId': 'fee78257-c71d-485f-b658-35f666bc20ad'
                    }
                }
                print(d['decisionType'])
                decisions.append(d)
            elif last_event['eventType'] == 'ActivityTaskCompleted':
                d = {'decisionType': 'CompleteWorkflowExecution',
                    'completeWorkflowExecutionDecisionAttributes': {
                        'result': 'Workflow execution completed'
                    }
                }
                print(d['decisionType'])
                decisions.append(d)
            elif last_event['eventType'] == 'ScheduleActivityTaskFailed':
                attributes = last_event['scheduleActivityTaskFailedEventAttributes']
                d = {'decisionType': 'FailWorkflowExecution',
                     'failWorkflowExecutionDecisionAttributes': {
                         'reason': 'ScheduleActivityTaskFailed',
                         'details': attributes['cause']
                     }
                }
                print(d['decisionType'])
                decisions.append(d)
            else:
                d = {'decisionType': 'ScheduleActivityTask',
                    'scheduleActivityTaskDecisionAttributes': {
                        'activityType': {
                            'name': 'PrintConsoleMessage',
                            'version': '1.1'
                            },
                        'activityId': 'fee78257-c71d-485f-b658-35f666bc20ad'
                    }
                }
                print(d['decisionType'])
                decisions.append(d)

            conn.respond_decision_task_completed(
                taskToken=response['taskToken'],
                decisions=decisions
            )
        except Exception as e:
            print('Exception: ', e)
