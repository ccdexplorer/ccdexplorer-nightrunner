ARG python_image_tag="3.11-slim-buster"
FROM python:${python_image_tag}
RUN apt-get update && apt-get install -y git gcc libpq-dev && rm -rf /var/lib/apt/lists/*
ARG CE_BOT_TOKEN
ENV CE_BOT_TOKEN ${CE_BOT_TOKEN}
WORKDIR /home/code
RUN cd /home/code

# Install Python dependencies.
COPY ./requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt
# Copy application files.
COPY . .
#
RUN git clone https://github.com/ccdexplorer/ccdexplorer-accounts.git /home/git_dir
RUN git config --global user.name "ceupdaterbot"
RUN git config --global user.email "bot@ccdexplorer.io"
RUN git config --global url.https://ceupdaterbot:{CE_BOT_TOKEN}@github.com/.insteadOf https://github.com/

CMD ["python3", "/home/code/main.py"]