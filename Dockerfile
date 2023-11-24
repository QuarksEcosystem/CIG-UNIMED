# Use the official Python base image with version 3.9
FROM python:3.9
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install --force-reinstall https://github.com/wbond/oscrypto/archive/d5f3437ed24257895ae1edd9e503cfb352e635a8.zip
RUN apt-get update
RUN apt-get install -y locales locales-all
ENV LC_ALL pt_BR.UTF-8
ENV LANG pt_BR.UTF-8
ENV LANGUAGE pt_BR.UTF-8
EXPOSE 8501
COPY . /app
COPY img /img 
ENTRYPOINT ["streamlit", "run"]               
CMD ["Main_Page.py"]