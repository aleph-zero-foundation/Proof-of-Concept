FROM python:3.7-alpine

# prepare a build environment
RUN apk update
RUN apk add --no-cache \
    bash \
	make \
	gcc \
	g++ \
	musl-dev \
	bison \
	flex \
	mpc \
	gmp-dev \
	openssl-dev \
	git \
	libpng-dev \
	libffi-dev \
	freetype-dev

WORKDIR /root
RUN wget https://crypto.stanford.edu/pbc/files/pbc-0.5.14.tar.gz
RUN tar -xvf pbc-0.5.14.tar.gz
RUN cd pbc-0.5.14 && ./configure && make >/dev/null 2>&1 && make install

RUN git clone https://github.com/JHUISI/charm.git
RUN cd charm && ./configure.sh && make install >/dev/null 2>&1

COPY setup.py /root
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -e .

# build succesfull

COPY aleph /root/aleph
WORKDIR /root/aleph

ENV N_CORES 1
ENV N 16

RUN rm -r __pycache__ unitTest/__pycache__

CMD pytest -n $N_CORES unitTest
