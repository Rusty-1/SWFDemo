import socket

__author__ = 'rusty'


import boto3

if __name__ == '__main__':

    print('Connecting to SWF ..')
    conn = boto3.client('swf')

    while True:
        try:
            print('Polling for activity task ...')
            task = conn.poll_for_activity_task(domain='SWF-Prototype', taskList={'name': 'PrintConsoleMessageTask'},
                                            identity='merada-1')

            print("***** CONSOLE MESSAGE *****")

            conn.respond_activity_task_completed(
                taskToken=task['taskToken'],
                result='success'
            )
        except Exception as e:
            print('Exception: ', e)

