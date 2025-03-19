# Info

## Male/female regex

vscode regex to search for is that the same string that ends the word before `$(#` is repeated as the first option inside the parenthesis:

```regex
\b\w*(\w+)\$\(#\1\/[^)]+\)
```

example:

```text
eres un hombre $(#hombre/mujer)
eres un hombre$(#hombre/mujer)
Bienvenido$(#o/a) de nuevo
```
