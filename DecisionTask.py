__author__ = 'rusty'


import boto3

if __name__ == '__main__':
    print('Connecting to SWF ...')
    conn = boto3.client('swf')

    while True:
        try:
            print('Polling for decision ...')
            response = conn.poll_for_decision_task(domain='SWF-Prototype', taskList={'name': 'ActivityTaskList'},
                                            identity='merada-1')

            if 'events' in response:
                # Find workflow events not related to decision scheduling
                workflow_events = [e for e in response['events'] if not e['eventType'].startswith('Decision')]
                last_event = workflow_events[-1]

                decisions = []
                if last_event['eventType'] == 'WorkflowExecutionStarted':
                    d = {'decisionType': 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes': {
                            'activityType': {
                                'name': 'Activity1',
                                'version': '1'
                                },
                            'activityId': 'activity1'
                        }
                    }
                    print(d['decisionType'] + ":Activity1")
                    decisions.append(d)
                elif last_event['eventType'] == 'ActivityTaskCompleted'\
                        and last_event['activityTaskCompletedEventAttributes']['result'] == 'Activity1_Complete':
                    d = {'decisionType': 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes': {
                            'activityType': {
                                'name': 'Activity2',
                                'version': '1'
                                },
                            'activityId': 'activity2'
                        }
                    }
                    print(d['decisionType'] + ":Activity2")
                    decisions.append(d)
                elif last_event['eventType'] == 'ActivityTaskCompleted'\
                        and last_event['activityTaskCompletedEventAttributes']['result'] == 'Activity2_Complete':
                    d = {'decisionType': 'CompleteWorkflowExecution',
                        'completeWorkflowExecutionDecisionAttributes': {
                            'result': 'Workflow execution completed'
                        }
                    }
                    print("Decision:" + d['decisionType'])
                    decisions.append(d)
                else:
                    d = {'decisionType': 'FailWorkflowExecution',
                        'failWorkflowExecutionDecisionAttributes': {
                            'reason': 'Workflow execution path failure'
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
