FROM python:3.5
ENV APP_ROOT /opt/scorer

WORKDIR $APP_ROOT

EXPOSE 5000
#CMD python run.py
CMD uwsgi --http 0.0.0.0:5000 --module run:app --master --processes 4 --threads 2

COPY ./requirements.txt $APP_ROOT
RUN pip install -r requirements.txt

COPY . $APP_ROOT

