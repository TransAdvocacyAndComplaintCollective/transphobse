from transformers import pipeline
from utils.keywords import KEYWORDS
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

text = "The UK government is introducing new laws affecting digital freedom."

result = classifier(text, KEYWORDS)
print(result)

result = classifier(text, KEYWORDS)
print(result)
