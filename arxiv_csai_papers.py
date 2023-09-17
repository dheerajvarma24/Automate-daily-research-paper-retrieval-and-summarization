from airflow.decorators import dag, task
import pendulum
import requests
import os
import slack
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from datetime import date
from extract_summary.extract_summary_hf import ExtractSummaryFBBart as FbBart
from extract_summary.extract_summary_gpt import ExtractSummaryGPT as GPT

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

ENV_PATH = os.path.join(os.getcwd(), '.env')
load_dotenv(dotenv_path=ENV_PATH)

PAPERS_URL = "https://arxiv.org/list/cs.AI/new"
PAPERS_DIR = os.getcwd() + "/papers/"
PAPERS_PATH = PAPERS_DIR + date.today().strftime("%d_%m_%Y") + "_new_papers.txt"

USE_GPT = True # GPT API's involves billing. If False it is configured to use HuggingFace's Bart model.


@dag(
    dag_id='arxiv_csai_papers_summary',
    schedule="@daily",
    start_date=pendulum.datetime(2023, 9, 4),
    catchup=False,
)

def arxiv_csai_papers_summary():

    @task()
    def get_papers():
        # create request
        page = requests.get(PAPERS_URL)
        soup = BeautifulSoup(page.text, 'html.parser')

        # Extract the title description
        title_list = soup.find_all('div', class_='list-title mathjax')
        titles = [title.get_text(strip=True, separator=' ') for title in title_list]
        print("First title is:", titles[0])

        # Extract the abstract
        abs_list = soup.find_all('p', class_="mathjax")
        abstracts = [abstract.get_text(strip=True, separator= ' ') for abstract in abs_list]
        print("First abstract is: ",abstracts[0])

        print("Number of papers found: %d" %(len(abs_list)))
        return [titles, abstracts]

    papers_titles_abstracts  = get_papers()

    create_database = SQLExecuteQueryOperator(
        task_id='create_table_sqlite',
        sql=r"""
        CREATE TABLE IF NOT EXISTS summary (
            title TEXT PRIMARY KEY,
            abstract TEXT
        );
        """,
        conn_id="summary"
        )        
    

    create_database.set_downstream(papers_titles_abstracts)

    # store the papers title and summary data into the database table.
    @task()
    def store_abstract(papers_titles_abstracts):
        hook = SqliteHook(sqlite_conn_id="summary")
        stored_summary = hook.get_pandas_df("SELECT * from summary;")
        print("Number of papers in the database: %d" %(len(stored_summary)))       

        new_summarys = []
        for title, abstract in zip(papers_titles_abstracts[0], papers_titles_abstracts[1]):
            if stored_summary.empty or title not in stored_summary["title"].values:
                new_summarys.append([title,abstract])
        hook.insert_rows(table='summary', rows=new_summarys, target_fields=["title", "abstract"])
        print("Number of new papers: %d" %(len(new_summarys)))
        return new_summarys

    store_abstract(papers_titles_abstracts)

    # extact the summary of the new papers
    @task()
    def extract_summary(title_abstract_list):
        if os.path.exists(PAPERS_PATH):
            print("Papers are summerised and saved in %s" %(PAPERS_PATH))
            return        
        summary_list = GPT().summarize_papers(title_abstract_list[1]) if USE_GPT else FbBart().summerize_papers(title_abstract_list[1])
        
        # append the title to the summary
        for i, summary in enumerate(summary_list):
            summary_list[i] = title_abstract_list[0][i] + "\n\n" + summary
        print("Extracted summaries of %d papers" %(len(summary_list)))               
        return summary_list
        
    papers_summary = extract_summary(papers_titles_abstracts)
    

    # create a task to save the new papers to a folder with today's date.
    @task()
    def save_summary(papers_summary):
        if os.path.exists(PAPERS_PATH):
            print("File exists papers are saved in %s" %(PAPERS_PATH))
            return
            
        with open(PAPERS_PATH, "w") as f:
            for summary in papers_summary:
                f.write(summary + "\n\n\n\n")
            f.close()
        print("New papers saved to %s" %(PAPERS_PATH))

    ss = save_summary(papers_summary)

    
    # create a task to send a slack message with the new papers.    
    @task()
    def send_slack_message():
        # read papers from the file
        if not os.path.exists(PAPERS_PATH):
            print("path doesnot exist %s. Todays papers are not yet there." %(PAPERS_PATH))
            return
                
        try:
            client = slack.WebClient(token=os.environ['SLACK_API_TOKEN'])
            with open(PAPERS_PATH, "r") as f:
                text = f.read()
                f.close()
            response = client.chat_postMessage(
                channel='#daily-arxiv-ml-ai-papers',
                text=text)
            if(response["ok"]):    
                print("Papers sent to Slack")
        except Exception as e:
            print("Error sending message to slack:", e)
            return

    ssm = send_slack_message()
    ss.set_downstream(ssm)


summary = arxiv_csai_papers_summary()
