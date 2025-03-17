import ee

ee.Authenticate()
ee.Initialize(project='ee-yangluhao990714')

tasks = ee.data.getTaskList()
for task in tasks:
    id = task['id']
    status = task['state']
    if status == 'RUNNING' or status == 'READY' or status == 'UNSUBMITTED':
        print(f'Cancelling task {id}')
        ee.data.cancelTask(id)