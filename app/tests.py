from io import BytesIO
from PyPDF2 import PdfReader, PdfWriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import fitz 

pdf = fitz.open("bill.pdf")
for i in range(pdf.page_count):
    page = pdf[i]
    text = page.get_text("text").split("Net Amount\n")
    if len(text) > 1 :
        print(text[1])
        text = text[1].split("\n")
        x = text[1] #first line description 
        for i in text[2:] : 
            if i == x : 
                print("error")
        [  for i in text ]


# add_image_to_pdf('bill.pdf', 'a.png' , 8, 24, 1.9, 1.9)
