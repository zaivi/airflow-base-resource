from urllib import parse

import pytz
from airflow.configuration import conf
from airflow.models import Variable
from airflow.utils.email import send_email


def _convert_to_hkt_timezone(dt):
    hk_timezone = pytz.timezone("Asia/Hong_Kong")
    execution_date_hk = dt.astimezone(hk_timezone)

    formatted_date = execution_date_hk.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_date


def _build_airflow_url(dag_id, run_id):
    base_url = conf.get("webserver", "base_url")
    view = conf.get("webserver", "dag_default_view").lower()
    return f"{base_url}/dags/{dag_id}/{view}?{parse.urlencode({'dag_run_id': run_id})}"


def _get_email_template_l1_v3():
    return """<!DOCTYPE html>
       <html>
       <head>
       <style>
           table {{
               width: 100%;
               border-collapse: collapse;
           }}
           table, td, th {{
               border: 1px solid #000;
           }}
           td, th {{
               padding: 8px;
               text-align: left;
           }}
           th {{
               background-color: #f2f2f2;
           }}
       </style>
       </head>
       <body>
       <table>
           <tr><th>SLA Priority</th><td>{sla_priority}</td></tr>
           <tr><th>Support Analyst</th><td>{support_analyst}</td></tr>
           <tr><th>Country/Banner</th><td>{country_banner}</td></tr>
           <tr><th>Prerequisite Job Name</th><td>Refer to Cloud Composer Error Message below</td></tr>
           <tr><th>Cloud Composer</th><td>{cloud_composer}</td></tr>
           <tr><th>Pipeline Name</th><td>{pipeline_name}</td></tr>
           <tr><th>Root DAG ID</th><td>{root_dag_id}</td></tr>
           <tr><th>Config Name Prefix</th><td>Refer to Cloud Composer Error Message below</td></tr>
           <tr><th>Pipeline Run Id</th><td>{pipeline_run_id}</td></tr>
           <tr><th>Pipeline Run Url</th><td><a href="{pipeline_run_url}">{pipeline_run_url}</a></td></tr>
           <tr><th>Pipeline Trigger Time</th><td>{pipeline_trigger_time}</td></tr>
           <tr><th>Pipeline Trigger Type</th><td>{pipeline_trigger_type}</td></tr>
           <tr><th>Message</th><td>{message}</td></tr>
           <tr><th>Instructions(for Monitoring team)</th><td>{instructions}</td></tr>
       </table>
       </body>
       </html>"""


def _get_email_template():
    return """<!DOCTYPE html>
       <html>
       <head>
       <style>
           table {{
               width: 100%;
               border-collapse: collapse;
           }}
           table, td, th {{
               border: 1px solid #000;
           }}
           td, th {{
               padding: 8px;
               text-align: left;
           }}
           th {{
               background-color: #f2f2f2;
           }}
       </style>
       </head>
       <body>
       <table>
           <tr><th>SLA Priority</th><td>{sla_priority}</td></tr>
           <tr><th>Support Analyst</th><td>{support_analyst}</td></tr>
           <tr><th>Country/Banner</th><td>{country_banner}</td></tr>
           <tr><th>Cloud Composer</th><td>{cloud_composer}</td></tr>
           <tr><th>Pipeline Name</th><td>{pipeline_name}</td></tr>
           <tr><th>Root DAG ID</th><td>{root_dag_id}</td></tr>
           <tr><th>Pipeline Run Id</th><td>{pipeline_run_id}</td></tr>
           <tr><th>Pipeline Run Url</th><td><a href="{pipeline_run_url}">{pipeline_run_url}</a></td></tr>
           <tr><th>Pipeline Trigger Time</th><td>{pipeline_trigger_time}</td></tr>
           <tr><th>Pipeline Trigger Type</th><td>{pipeline_trigger_type}</td></tr>
           <tr><th>Message</th><td>{message}</td></tr>
       </table>
       </body>
       </html>"""


def _send_successful_email_notification(**kwargs):
    dag_id = kwargs.get("dag_id")
    run_id = kwargs["dag_run"].run_id
    run_url = _build_airflow_url(dag_id, run_id)
    root_dag_id = kwargs["dag_run"].conf.get("root_dag_id", "None")
    is_email_active = kwargs.get("is_email_active")
    email = kwargs.get("email")
    email_cc = kwargs.get("email_cc")
    email_subject_prefix = kwargs.get("email_subject_prefix").replace("##DAG_ID##", dag_id)
    sla_priority = kwargs.get("sla_priority")
    support_analyst = kwargs.get("support_analyst")
    country_region = kwargs.get("country_region")
    banner = kwargs.get("banner")
    country_banner = f"{country_region}/{banner}"
    email_template = _get_email_template()

    # Replace placeholders with actual values
    formatted_email = email_template.format(
        sla_priority=sla_priority,
        support_analyst=support_analyst,
        country_banner=country_banner,
        cloud_composer=Variable.get(key="composer_name", default_var=None),
        pipeline_name=dag_id,
        root_dag_id=root_dag_id,
        pipeline_run_id=run_id,
        pipeline_run_url=run_url,
        pipeline_trigger_time=_convert_to_hkt_timezone(kwargs["execution_date"]),
        pipeline_trigger_type="manual" if run_id.startswith("manual__") else "scheduled",
        message=f"{dag_id} successfully ran",
    )

    email_subject = f"{email_subject_prefix} Succeeded "

    if is_email_active:
        send_email(to=email, cc=email_cc, subject=email_subject, html_content=formatted_email)
    else:
        print(f"Email is not active for DAG {dag_id}")


# send alert email motication especially for L1_v3
def _send_alert_email_notification_l1_v3(**kwargs):
    dag_id = kwargs.get("dag_id")
    run_id = kwargs["dag_run"].run_id
    run_url = _build_airflow_url(dag_id, run_id)
    root_dag_id = kwargs["dag_run"].conf.get("root_dag_id", "None")
    email = kwargs.get("email")
    email_cc = kwargs.get("email_cc")
    email_message = kwargs.get("email_message")
    instructions = kwargs.get("instructions")
    country_region = kwargs["dag_run"].conf.get("country_region")
    banner = kwargs["dag_run"].conf.get("banner")
    country_banner = f"{country_region}/{banner}"
    email_template = _get_email_template_l1_v3()

    # Replace placeholders with actual values
    formatted_email = email_template.format(
        sla_priority="#sla_priority#",
        support_analyst="#support_analyst#",
        country_banner=country_banner,
        cloud_composer=Variable.get(key="composer_name", default_var=None),
        pipeline_name=dag_id,
        root_dag_id=root_dag_id,
        pipeline_run_id=run_id,
        pipeline_run_url=run_url,
        pipeline_trigger_time=kwargs["execution_date"].strftime("%Y-%m-%d %H:%M:%S"),
        pipeline_trigger_type="manual" if run_id.startswith("manual__") else "scheduled",
        message=email_message,
        instructions=instructions,
    )
    # Open Ticket: [Datalake PD] SGFOM Ingestion Monitoring (Daily_Batch_1000) Failed
    email_subject = f"Open Ticket: [Datalake] {country_banner} Ingestion Monitoring Failed"
    send_email(to=email, cc=email_cc, subject=email_subject, html_content=formatted_email)


# send alert email motication based on the customised email, provided the parameters by task level
def _send_alert_email_notification(**kwargs):
    dag_id = kwargs.get("dag_id")
    run_id = kwargs["dag_run"].run_id
    run_url = _build_airflow_url(dag_id, run_id)
    root_dag_id = kwargs["dag_run"].conf.get("root_dag_id", "None")
    support_analyst = kwargs.get("support_analyst")
    sla_priority = kwargs.get("sla_priority")
    is_email_active = kwargs.get("is_email_active")
    email = kwargs.get("email")
    email_cc = kwargs.get("email_cc")
    email_message = kwargs.get("email_message")
    country_region = kwargs["dag_run"].conf.get("country_region")
    banner = kwargs["dag_run"].conf.get("banner")
    country_banner = kwargs.get("country_banner") if kwargs.get("country_banner") else f"{country_region}/{banner}"
    email_template = _get_email_template()

    # Replace placeholders with actual values
    formatted_email = email_template.format(
        sla_priority=sla_priority,
        support_analyst=support_analyst,
        country_banner=country_banner,
        cloud_composer=Variable.get(key="composer_name", default_var=None),
        pipeline_name=dag_id,
        root_dag_id=root_dag_id,
        pipeline_run_id=run_id,
        pipeline_run_url=run_url,
        pipeline_trigger_time=kwargs["execution_date"].strftime("%Y-%m-%d %H:%M:%S"),
        pipeline_trigger_type="manual" if run_id.startswith("manual__") else "scheduled",
        message=email_message,
    )

    email_subject = kwargs.get("email_subject")

    if is_email_active:
        send_email(to=email, cc=email_cc, subject=email_subject, html_content=formatted_email)
    else:
        print(f"Email is not active for DAG {dag_id}")


# failure callback for task level
def _send_failure_callback_email_notification(context):
    dag_id = context["dag_run"].dag_id
    run_id = context["dag_run"].run_id
    run_url = _build_airflow_url(dag_id, run_id)
    root_dag_id = context["params"].get("root_dag_id", "None")
    email = ["chenglong.wu@dfiretailgroup.com", "zhengwei.ng@dfiretailgroup.com", "jeff.chen@dfiretailgroup.com"]
    email_cc = ["chanyee.leong@dfiretailgroup.com"]
    email_subject_prefix = "[Datalake]  ##DAG_ID## Monitoring -".replace("##DAG_ID##", dag_id)
    email_template = _get_email_template()

    formatted_email = email_template.format(
        sla_priority="#sla_priority#",
        support_analyst="#support_analyst#",
        country_banner=f"{context['params'].get('country_region')}/{context['params'].get('banner')}",
        cloud_composer=Variable.get(key="composer_name", default_var=None),
        pipeline_name=dag_id,
        root_dag_id=root_dag_id,
        pipeline_run_id=run_id,
        pipeline_run_url=run_url,
        pipeline_trigger_time=context["execution_date"].strftime("%Y-%m-%d %H:%M:%S"),
        pipeline_trigger_type="manual" if run_id.startswith("manual__") else "scheduled",
        message=f"{dag_id} failed, exception: {context['exception']}",
    )

    email_subject = f"{email_subject_prefix} Failed "

    send_email(to=email, cc=email_cc, subject=email_subject, html_content=formatted_email)


# failure callback for DAG level
def _send_failure_callback_email_notification_main(context, email, email_cc):
    dag_id = context["dag_run"].dag_id
    run_id = context["dag_run"].run_id
    run_url = _build_airflow_url(dag_id, run_id)
    root_dag_id = context["params"].get("root_dag_id", "None")
    email_subject_prefix = "[Datalake]  ##DAG_ID## Monitoring -".replace("##DAG_ID##", dag_id)
    email_template = _get_email_template()

    formatted_email = email_template.format(
        sla_priority="#sla_priority#",
        support_analyst="#support_analyst#",
        country_banner=f"{context['params'].get('country_region')}/{context['params'].get('banner')}",
        cloud_composer=Variable.get(key="composer_name", default_var=None),
        pipeline_name=dag_id,
        root_dag_id=root_dag_id,
        pipeline_run_id=run_id,
        pipeline_run_url=run_url,
        pipeline_trigger_time=context["execution_date"].strftime("%Y-%m-%d %H:%M:%S"),
        pipeline_trigger_type="manual" if run_id.startswith("manual__") else "scheduled",
        message=f"{dag_id} failed, exception: {context['exception']}",
    )

    email_subject = f"{email_subject_prefix} Failed "

    send_email(to=email, cc=email_cc, subject=email_subject, html_content=formatted_email)


# SLA missed callback
def _send_sla_missed_callback_email_notifictaion(*args, email, email_cc, email_subject, email_message):
    dag = args[0]
    dag_id = dag.dag_id
    latest_execution_date = dag.latest_execution_date
    run_id = f"scheduled__{latest_execution_date.isoformat()}"
    run_url = _build_airflow_url(dag_id, run_id)
    hkt_timezone = pytz.timezone("Asia/Hong_Kong")
    email_template = _get_email_template()

    # formatted_email = email_template.format(
    #     sla_priority="",
    #     support_analyst="",
    #     # country_banner=f"{context['params'].get('country_region')}/{context['params'].get('banner')}",
    #     country_banner="",
    #     cloud_composer=Variable.get(key="composer_name", default_var=None),
    #     pipeline_name=dag_id,
    #     # root_dag_id=root_dag_id,
    #     root_dag_id="",
    #     pipeline_run_id=run_id,
    #     pipeline_run_url=run_url,
    #     pipeline_trigger_time=latest_execution_date.astimezone(hkt_timezone).strftime("%Y-%m-%d %H:%M:%S"),
    #     pipeline_trigger_type="manual" if run_id.startswith("manual__") else "scheduled",
    #     message=f"{dag_id} SLA misseddddddddddddddddddddddd",
    # )

    send_email(to=email, cc=email_cc, subject=email_subject, html_content=email_message)
