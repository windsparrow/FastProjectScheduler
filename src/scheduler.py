import collections
import ast
import plotly
from pathlib import Path
from ortools.sat.python import cp_model
import plotly.express as px
import pandas as pd



# find the best work schedule with google ortools
#the problem is enssentially a flexible job shop problem
def flexible_job_shop(plan_df,resource_df):
    # group the tasks in one job
    jobs = []
    plan_df['prj_job'] = plan_df.project_name + '#' + plan_df.job
    for prj_job in plan_df.prj_job.unique():
        onejob = []
        for k,v in plan_df[ plan_df.prj_job == prj_job ].iterrows():
            onejob.append( ( prj_job + '#' + v.task , v.resource , v['duration(day)'] , prj_job , ast.literal_eval(v.preorder_task_index)) )
        jobs.append(onejob)

    # construct resource dict
    res_dict = {}
    for res in resource_df.resource_type.unique():
        res_list = []
        for k,v in resource_df[ resource_df.resource_type == res ].iterrows():
            res_list.append( v.resource_name )
        res_dict[res] = res_list
    all_resources = resource_df.resource_name.to_list()


    num_jobs = len(jobs)
    all_jobs = range(num_jobs)

    model = cp_model.CpModel()

    # find the latest end time of the plan
    horizon = 0
    for job in jobs:
        for task in job:
            horizon += task[2]

    print('Horizon = %i' % horizon)

    # Named tuple to store information about created variables.
    task_type = collections.namedtuple('task_type', 'start end interval preorder')
    # Named tuple to manipulate solution information.
    assigned_task_type = collections.namedtuple('assigned_task_type',
                                                'start job index duration')

    # Creates job intervals and add to the corresponding machine lists.
    all_tasks = {}
    #the tasks for each resource
    intervals_per_resources = collections.defaultdict(list)
    presences = {}  # indexed by (job_id, task_id, alt_id).

    for job_id, job in enumerate(jobs):
        for task_id, task in enumerate(job):
            resource_type = task[1]
            duration = task[2]
            suffix = '_%i_%i' % (job_id, task_id)
            start_var = model.NewIntVar(0, horizon, 'start' + suffix)
            end_var = model.NewIntVar(0, horizon, 'end' + suffix)
            interval_var = model.NewIntervalVar(start_var, duration, end_var,
                                                'interval' + suffix)
            all_tasks[job_id, task_id] = task_type(start=start_var,
                                                    end=end_var,
                                                    interval=interval_var,
                                                    preorder = task[4])

            # Create alternative intervals.
            num_alternatives = len(res_dict[ resource_type ])
            all_alternatives = res_dict[ resource_type ]
            if num_alternatives > 1:
                l_presences = []
                for alt_id in all_alternatives:  #alt_id is resource_name
                    alt_suffix = '_j%i_t%i_a%s' % (job_id, task_id, alt_id)
                    l_presence = model.NewBoolVar('presence' + alt_suffix)
                    l_start = model.NewIntVar(0, horizon, 'start' + alt_suffix)
                    l_duration = duration
                    l_end = model.NewIntVar(0, horizon, 'end' + alt_suffix)
                    l_interval = model.NewOptionalIntervalVar(
                        l_start, l_duration, l_end, l_presence,
                        'interval' + alt_suffix)
                    l_presences.append(l_presence)

                    # Link the master variables with the local ones.
                    model.Add(start_var == l_start).OnlyEnforceIf(l_presence)
                    model.Add(end_var == l_end).OnlyEnforceIf(l_presence)

                    # Add the local interval to the right machine.
                    intervals_per_resources[alt_id].append(l_interval)

                    # Store the presences for the solution.
                    presences[(job_id, task_id, alt_id)] = l_presence

                # Select exactly one presence variable.
                model.AddExactlyOne(l_presences)
            else:
                alt_id = all_alternatives[0]
                intervals_per_resources[alt_id].append(interval_var)
                #print('debug',alt_id,interval_var)
                presences[(job_id, task_id, alt_id)] = model.NewConstant(1)


            #resource_to_intervals[resource].append(interval_var)

    # Create and add disjunctive constraints.
    # one resource can only work on one task
    for res_name in all_resources:
        intervals = intervals_per_resources[res_name]
        #print('intervals', res_name , intervals)
        if len(intervals) > 1:
            model.AddNoOverlap(intervals)

    # Precedences inside a job.
    for job_id, job in enumerate(jobs):
        for task_id in range(len(job) - 1):
            preorders = all_tasks[job_id, task_id + 1].preorder
            for pre in preorders:
                model.Add(all_tasks[job_id, task_id +
                                    1].start >= all_tasks[job_id, pre ].end)

    # Makespan objective.
    makespan = model.NewIntVar(0, horizon, 'makespan')
    model.AddMaxEquality(makespan, [
        all_tasks[job_id, len(job) - 1].end
        for job_id, job in enumerate(jobs)
    ])
    model.Minimize(makespan)

    # Creates the solver and solve.
    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        job_schedule = []
        for job_id, job in enumerate(jobs):
            for task_id, task in enumerate(job):
                start_time = solver.Value( all_tasks[job_id,task_id].start )
                duration = task[2]
                end_time = solver.Value( all_tasks[job_id,task_id].end )
                presence_resource = 'Unkown'
                resource_type = task[1]
                for resource_name in res_dict[ resource_type ]:
                    if solver.Value(presences[(job_id, task_id, resource_name)]) : 
                        presence_resource = resource_name
                #( task name, resource name, start time , end time  , prj_job, resource_name,'project_name','job','task')
                job_schedule.append(( task[0],task[1],start_time,end_time ,task[3],presence_resource,task[2],* task[0].split('#')))
    return job_schedule

# convert the schedule to a date plan , without holidays
def schedule_only_workdays(job_schedule , start_date , output_schedule_path):
    df = pd.DataFrame(job_schedule,columns=['task_name', 'resource_type', 'start_time' , 'end_time' ,'prj_job','resource_name','duration','project_name','job','task'])
    # make up workday list
    work_dates = pd.bdate_range(start_date, periods=df.end_time.max()+1)
    df.start_time = df.apply( lambda x : work_dates[x.start_time] , axis=1)
    df.end_time = df.apply( lambda x : work_dates[x.end_time] , axis=1)
    df = df[['task_name', 'resource_type', 'resource_name','duration','start_time' , 'end_time' ,'prj_job','project_name','job','task']]
    df.to_excel(output_schedule_path)
    return df



#use plotly to plot gantt
def plot_gantt( schedule_df , fig_name ):
    fig = px.timeline(df, x_start="start_time", x_end="end_time", y="resource_name",color='prj_job',hover_name='task_name')
    plotly.offline.plot(fig , filename = fig_name)


if __name__ == '__main__':
    base_path = Path(__file__).parent
    input_data_path = (base_path / "../data-input/project-task-list.xlsx").resolve()

    plan_df = pd.read_excel(input_data_path ,sheet_name='job-list')  # read the job list in the projects
    resource_df = pd.read_excel(input_data_path ,sheet_name='resources') # read the available resources
    job_schedule = flexible_job_shop(plan_df,resource_df)

    start_date = '2023-02-26'
    output_schedule_path = (base_path / '../schedule-output/schedule_list.xlsx').resolve()

    df = schedule_only_workdays(job_schedule , start_date , output_schedule_path)

    fig_name = str( (base_path / '../schedule-output/schedule_gantt.html').resolve() )
    plot_gantt(df,fig_name)

    print('All Done!')


