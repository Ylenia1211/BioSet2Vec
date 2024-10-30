# BioSet2Vec

**BioSet2Vec** is a tool designed to extract k-mer dictionaries from multiple sets of biological sequences using distributed computing. This method is efficient for large-scale biological sequence analysis, enabling users to handle diverse sequence sets, such as DNA sequences, and extract k-mer representations in a distributed fashion. The extracted k-mer dictionaries can be used for downstream tasks like sequence comparison, feature extraction, and machine learning.

## Features

- **Distributed k-mer extraction**: Handles multiple biological sequence sets.
- **Scalability**: Optimized for large datasets using distributed computation.
- **Flexible k-mer size**: Adjustable k-mer lengths to suit the user’s specific needs.
- **Support for various biological sequences**: Compatible with DNA sequences.
- **Efficient I/O handling**: Works with large files in FASTA, FASTQ, or other common biological sequence formats.

## Requirements

- Python ">=3.8,<3.9"
- [Apache Spark](https://spark.apache.org/) (for distributed computing)
- https://github.com/Ylenia1211/Bioft.git
- Additional dependencies listed in `requirements.txt`


Operating system(s): Platform independent

## Installation

1. Clone the repository:

    ```bash
    git clone [https://github.com/yourusername/BioSet2Vec.git](https://github.com/Ylenia1211/BioSet2Vec.git)
    cd BioSet2Vec
    ```

2. Install the required Python dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Set up Apache Spark:

    Follow the [official guide](https://spark.apache.org/docs/latest/index.html) to set up Apache Spark in your environment.
