FROM python:3.12-slim

WORKDIR /app

# Install git for cloning repos
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install chronos dependencies explicitly
RUN pip install --no-cache-dir \
    numpy==2.4.3 \
    pandas==2.3.3 \
    tensorflow==2.21.0 \
    h5py==3.12.1 \
    patsy==0.5.6 \
    matplotlib==3.9.2 \
    seaborn==0.13.2 \
    scikit-learn==1.5.2 \
    statsmodels==0.14.5 \
    scipy==1.16.3 \
    adjustText==1.2.0 \
    umap-learn==0.5.6 \
    reportlab==4.2.2 \
    sympy==1.13.2

# Install chronos from GitHub (no deps since we installed them above)
RUN pip install --no-cache-dir --no-deps git+https://github.com/broadinstitute/chronos.git

# Install FastAPI server dependencies
RUN pip install --no-cache-dir \
    fastapi==0.135.2 \
    uvicorn==0.42.0 \
    python-multipart==0.0.22 \
    pypdf==6.9.2

# Clone the backend repo
RUN git clone https://github.com/broadinstitute/chronos_portal_backend.git .

# Create directories for job data
RUN mkdir -p Jobs Logs

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
