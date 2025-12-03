import zmq
import sys

def main():
    # Wir erwarten ein Argument, um zu wissen, ob dies Reducer 1 oder 2 ist
    if len(sys.argv) < 2:
        print("Fehler: Bitte ID angeben (python reducer.py 1)")
        return

    my_id = sys.argv[1]
    
    # Port auswählen: Reducer 1 -> 5558, Reducer 2 -> 5559
    if my_id == "1":
        port = "5558"
    else:
        port = "5559"

    context = zmq.Context()

    # 1. PULL-Socket zum Empfangen der Wörter von den Mappern
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://*:{port}")

    print(f"Reducer {my_id} gestartet auf Port {port}. Warte auf Wörter...")

    # Lokaler Zähler (Dictionary) für Wortanzahlen (zählt für jedes vorkommende Wort die Anzahl)
    word_count = {}

    while True:
        # Empfange ein Wort
        word = receiver.recv_string()
        print(f"Reducer {my_id} empfangen: {word}")

        # Zähle das Wort
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

        # Ausgabe des aktuellen Standes für das empfangene Wort
        print(f"[Reducer {my_id}] {word}: {word_count[word]}")

if __name__ == "__main__":
    main()