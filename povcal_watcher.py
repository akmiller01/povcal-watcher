import requests
import pandas as pd
import io
import os
import datetime
import json
import smtplib
from email.mime.text import MIMEText

def fetch_data():
    """Fetch data from PovCalNet at $1.90 poverty line."""
    url = "http://iresearch.worldbank.org/PovcalNet/PovcalNetAPI.ashx?"
    smy_params = {
        "Countries": "all",
        "GroupedBy": "WB",
        "PovertyLine": "1.9",
        "RefYears": "all",
        "Display": "C",
        "format": "csv"
    }
    agg_params = {
        "Countries": "all",
        "GroupedBy": "WB",
        "PovertyLine": "1.9",
        "RefYears": "all",
        "Display": "Regional",
        "format": "csv"
    }

    smy_url = url + "&".join(["{}={}".format(item[0], item[1]) for item in smy_params.items()])
    agg_url = url + "&".join(["{}={}".format(item[0], item[1]) for item in agg_params.items()])

    s_response = requests.get(url=smy_url).content
    a_response = requests.get(url=agg_url).content

    smy_data = pd.read_csv(io.StringIO(s_response.decode('utf-8')))
    agg_data = pd.read_csv(io.StringIO(a_response.decode('utf-8')))

    return agg_data, smy_data


def record_data(agg_data, smy_data):
    """If data is new, record it to hard disk."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    new_dir = os.path.join(dir_path, datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    os.makedirs(new_dir)
    agg_data.to_pickle(os.path.join(new_dir, "agg.pkl"))
    smy_data.to_pickle(os.path.join(new_dir, "smy.pkl"))
    agg_data.to_csv(os.path.join(new_dir, "agg.csv"), index=False)
    smy_data.to_csv(os.path.join(new_dir, "smy.csv"), index=False)


def data_is_the_same(new_agg, new_smy):
    """Check whether data has not changed."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    all_subdirs = [d for d in os.listdir(dir_path) if os.path.isdir(d) and d != ".git"]
    if len(all_subdirs) == 0:
        agg_data, smy_data = fetch_data()
        record_data(agg_data, smy_data)
        return True, agg_data, smy_data
    latest_subdir = max(all_subdirs, key=os.path.getmtime)
    old_agg = pd.read_pickle(os.path.join(dir_path, latest_subdir, "agg.pkl"))
    old_smy = pd.read_pickle(os.path.join(dir_path, latest_subdir, "smy.pkl"))
    return (new_agg.equals(old_agg) and new_smy.equals(old_smy)), old_agg, old_smy


def send_email(subject, message):
    """Send a notice"""
    conf = json.load(open("mail_conf.json"))
    fromEmail = conf["email1"]
    fromEmailPassword = conf["email1password"]
    recipients = conf["recipients"]

    message_wrapper = """\
    <html>
        <head></head>
        <body>
            {}
        </body>
    </html>
    """.format(message)

    msg = MIMEText(message_wrapper, 'html')
    msg['Subject'] = subject
    msg['From'] = fromEmail
    msg['To'] = ", ".join(recipients)

    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.starttls()
    smtp.login(fromEmail, fromEmailPassword)
    smtp.sendmail(fromEmail, recipients, msg.as_string())
    smtp.quit()


def main():
    try:
        print("Fetching data...")
        agg_data, smy_data = fetch_data()
    except Exception as e:
        print("Encountered an error fetching data...")
        send_email("PovCalNet data fetch has failed", "<p>Error message: "+str(e)+"</p>")
    its_the_same, old_agg, old_smy = data_is_the_same(agg_data, smy_data)
    if its_the_same:
        print("Data is the same!")
    else:
        print("Data is not the same!")
        record_data(agg_data, smy_data)
        send_email(
            "PovCalNet has been updated",
            """\
            <p>The PovCal Watcher has detected a change in PovCalNet.</p>
            <h2>Previous aggregate table</h2>
            {}
            </hr>
            <h2>New aggregate table</h2>
            {}
            """.format(old_agg.to_html(), agg_data.to_html())
        )


if __name__ == '__main__':
    main()
