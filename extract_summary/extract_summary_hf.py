from transformers import pipeline

class ExtractSummaryFBBart:
    def __init__(self):
        self.model = "facebook/bart-large-cnn"
        self.tokenizer = "facebook/bart-large-cnn"
        try:
            self.summarizer = pipeline("summarization", model=self.model, tokenizer=self.tokenizer)
        except Exception as e:
            print("Error in loading the summerizer from HF: ", e)
            return


    def extract_summary(self, text):
        summary = self.summarizer(text, max_length=100, min_length=30, do_sample=False)
        return summary[0]['summary_text']
    
    def summerize_papers(self, papers):
        summary_list = []
        for paper in papers:
            summary_list.append(self.extract_summary(paper))
        return summary_list
