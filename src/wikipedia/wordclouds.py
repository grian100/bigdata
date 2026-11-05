"""Nuvole di parole per categoria con la libreria WordCloud."""

from pathlib import Path

import matplotlib.pyplot as plt
from wordcloud import WordCloud

from .data import LABEL


def wordcloud_image(frequencies, width=800, height=400):
    """Immagine della nuvola a partire da un dizionario parola -> peso."""
    cloud = WordCloud(width=width, height=height, background_color="white", colormap="viridis",
                      max_words=len(frequencies), random_state=0)
    return cloud.generate_from_frequencies(frequencies)


def frequencies_by_category(words, weight="score"):
    """Da una tabella (categoria, word, peso) a un dizionario per categoria."""
    return {
        category: dict(zip(group["word"], group[weight]))
        for category, group in words.groupby(LABEL)
    }


def save_wordclouds(words, directory, weight="score"):
    """Salva una PNG per categoria e restituisce i percorsi."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    paths = []
    for category, frequencies in frequencies_by_category(words, weight).items():
        path = directory / f"{category}.png"
        wordcloud_image(frequencies).to_file(str(path))
        paths.append(path)
    return paths


def wordcloud_grid(words, weight="score", columns=3):
    """Figura matplotlib con le nuvole di tutte le categorie."""
    frequencies = frequencies_by_category(words, weight)
    rows = -(-len(frequencies) // columns)
    fig, axes = plt.subplots(rows, columns, figsize=(6 * columns, 3.2 * rows))
    for ax in axes.flat:
        ax.axis("off")
    for ax, (category, freq) in zip(axes.flat, frequencies.items()):
        ax.imshow(wordcloud_image(freq, 600, 300), interpolation="bilinear")
        ax.set_title(category, fontsize=14)
    fig.tight_layout()
    return fig
