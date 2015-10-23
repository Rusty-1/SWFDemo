import socket

__author__ = 'rusty'


import boto3

if __name__ == '__main__':

    print('Connecting to SWF ...')
    conn = boto3.client('swf')

    while True:
        try:
            print('Polling for activity task ...')
            response = conn.poll_for_activity_task(domain='SWF-Prototype', taskList={'name': 'ActivityTaskList'},
                                            identity='merada-1')

            print("Hello World!")

            conn.respond_activity_task_completed(
                taskToken=response['taskToken'],
                result='Activity1_Complete'
            )
        except Exception as e:
            print('Exception: ', e)

