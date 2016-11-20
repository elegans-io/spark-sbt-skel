# Sample sbt project for spark

## Build

Note: works with java 7 and 8 (not with jdk 9)

sbt package

## Classes

### io.elegans.exercises.TokenizeSentences

#### running using sparkSubmit plugin of sbt

```bash
sbt "sparkSubmit --class io.elegans.exercises.TokenizeSentences -- --help"

Tokenize a list of sentences with spark
Usage: TokenizeSentences [options]

  --help              prints this usage text
  --input <value>   the input file or directory with input text  default: sentences.txt
  --output <value>  the destination directory for the output  default: TOKENIZED_SENTENCES
```

#### running calling spark-submit

```bash
./scripts/run.sh io.elegans.exercises.TokenizeSentences --help
```

e.g.

```bash
./scripts/run.sh io.elegans.exercises.TokenizeSentences --input sentences.utf8.clean.txt  --output TOKENIZED
```

