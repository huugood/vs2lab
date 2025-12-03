# Function: Task ventilator
# Binds PUSH socket to tcp://localhost:5557
# Sends messages to mapper via that socket

import zmq
import time
import sys
import random
import os

def main ():
    context = zmq.Context()

    # PUSH-Socket zum Senden der Aufgaben (Sätze) an die Mapper
    # Wir binden an Port 5557 (wie im C-Beispiel der Ventilator)
    sender = context.socket(zmq.PUSH)
    sender.bind("tcp://*:5557")

    print("Splitter (Ventilator) gestartet auf Port 5557.")
    print("Drücke Enter, sobald die Mapper und Reducer bereit sind...")
    input()  # Warten, um das "Slow Joiner" Problem zu vermeiden, da sonst Sätze verloren gehen können
    print("Sende Aufgaben...")

    # Prüfen, ob eine Datei als Argument übergeben wurde
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        if os.path.exists(filename):
            print(f"Lese Sätze aus Datei: {filename}")
            send_from_file(sender, filename)
        else:
            print(f"Fehler: Datei '{filename}' nicht gefunden.")
            return
    else:
        print("Keine Datei angegeben. Generiere Zufallssätze...")
        send_generated_sentences(sender)

    # Kurze Pause zum Leeren des Puffers
    time.sleep(1)
    print("Fertig mit Senden.")

def send_from_file(socket, filename):
    """Liest eine Datei Zeile für Zeile und sendet sie."""
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip() # Leerzeichen und Zeilenumbrüche entfernen
            if line: # Leere Zeilen überspringen
                socket.send_string(line)
                print(f"Gesendet (Datei): {line}")
                # Kleine Bremse, damit wir im Terminal mitlesen können
                time.sleep(0.05)

def send_generated_sentences(socket):
    """Generiert neue Sätze aus einem Wörter-Pool und sendet sie."""
    # Unser Vokabular
    vocabulary = [
        "hallo", "welt", "test", "aufgabe", "funktioniert", 
        "cool", "yay", "wow", "unfassbar", "beeindruckend",
        "zeromq", "pipeline", "map", "reduce", "verteilt", 
        "systeme", "informatik", "labor", "worker", "sink",
        "ventilator", "socket", "context", "push", "pull",
        "nachricht", "server", "client", "cluster", "knoten",
        "schnell", "einfach", "skalierbar", "effizient", "robust",
        "asynchron", "parallel", "komplex", "digital", "virtuell",
        "automatisch", "synchron", "blockierend", "sicher",
        "senden", "empfangen", "rechnen", "verarbeitet", "läuft",
        "starten", "stoppen", "warten", "binden", "verbinden",
        "crashen", "speichern", "laden", "zählen",
        "daten", "python", "code", "bug", "feature", "skript",
        "prozess", "thread", "latenz", "bandbreite", "protokoll",
        "fehler", "lösung", "cpu", "ram", "netzwerk",
        "ist", "sind", "hat", "haben", "wird", "wurde",
        "der", "die", "das", "ein", "eine", "einer",
        "und", "oder", "aber", "wenn", "dann", "weil",
        "zu", "in", "für", "mit", "auf", "aus", "von",
        "nicht", "sehr", "viel", "wenig", "alles", "nichts",
        "super", "klasse", "genial", "schlecht", "ok"
    ]
    
    # Wir generieren 100 Sätze
    for i in range(100):
        # Zufällige Satzlänge zwischen 3 und 7 Wörtern
        length = random.randint(3, 7)
        
        # Wähle 'length' zufällige Wörter aus dem Vokabular
        # (random.choices erlaubt Wiederholungen, random.sample wäre ohne)
        words = random.choices(vocabulary, k=length)
        
        # Baue den Satz zusammen
        sentence = " ".join(words)
        
        socket.send_string(sentence)
        print(f"Gesendet ({i+1}/100): {sentence}")
        
        # Kleine Pause, damit es nicht zu schnell durchläuft (Simulation von Rechenzeit)
        time.sleep(0.05)

if __name__ == "__main__":
    main()