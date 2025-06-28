Exactly! In your setup, sleb128 is essentially redundant because zigzag encoding converts signed integers into a form that unsigned LEB128 can efficiently encode.

So the combo:

Zigzag (signed → unsigned)

Unsigned LEB128

covers the same ground that sleb128 would handle alone, but often more efficiently.

Summary:
Zigzag + unsigned LEB128 = covers signed integer compression well, making sleb128 unnecessary in your pipeline.