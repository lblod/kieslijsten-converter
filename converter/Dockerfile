FROM ruby:2.5
WORKDIR /app
COPY Gemfile /app
RUN bundle
COPY . /app
ENV INPUT_DATE_FORMAT "%d/%m/%Y"
CMD "/app/convert-to-ttl.rb"