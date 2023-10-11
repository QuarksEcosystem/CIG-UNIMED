# Use the official Python base image with version 3.9
FROM python:3.9
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get install -y locales locales-all
ENV LC_ALL pt_BR.UTF-8
ENV LANG pt_BR.UTF-8
ENV LANGUAGE pt_BR.UTF-8
EXPOSE 8501
COPY . /app
ENTRYPOINT ["streamlit", "run"]               
CMD ["Main_Page.py"]