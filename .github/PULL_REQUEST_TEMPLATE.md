## Qué cambia

Una o dos frases. Qué hace el cambio y por qué hacía falta.

## Alcance

Qué capacidad toca (`repository`, `restclient`, `cache`, `security`, `storage`,
`resilience`, `api`, `bootstrap`). Si toca más de una, explicá por qué no se
pudieron separar.

## Evidencia

Pegá la salida real, no la descripción de la salida. "Funciona" no es evidencia.

- **Suite de tests.** La línea `N passed` de `pytest -q`, y el número que había
  antes de tu cambio, para que se vea la diferencia.
- **Los tests que agregaste.** Sus nombres y qué caso cubre cada uno. Un cambio
  sin tests nuevos necesita una razón acá.
- **Dependencias opcionales.** Si tocaste algo que vive detrás de un extra,
  confirmá que `tests/test_capabilities.py` sigue pasando: importar
  `persistence_kit` no puede arrastrar `fastapi`, `pyjwt` ni `boto3`.
- **Probado contra una app real.** Si lo corriste desde un proyecto que consume
  el paquete con install editable, el comando que corriste y qué observaste.
  Sirven capturas o líneas de log.
- **Cambio de comportamiento.** Si algo se comporta distinto que antes, mostrá
  los dos lados: qué hacía antes y qué hace ahora.

## Impacto sobre quien consume el kit

- ¿Exporta nombres nuevos desde `persistence_kit`? Listalos.
- ¿Suma una dependencia opcional o un extra? Nombralo y decí a qué extra pertenece.
- ¿Rompe a alguien que esté en la versión anterior? Decilo derecho y explicá qué
  tiene que cambiar esa persona.
- ¿Necesita subir la versión, y de qué tipo?

## Checklist

- [ ] Primero el contrato: `Protocol` para capacidades nuevas, `ABC` solo para el par de repositorios
- [ ] Todo `async`, argumentos keyword-only
- [ ] Implementación de memoria junto a la real
- [ ] La elección pasa por un factory que lee settings, con el provider importado de forma perezosa
- [ ] Los errores cuelgan de la excepción base del paquete
- [ ] Exportado de forma perezosa en `_OPTIONAL_EXPORTS` si necesita una dependencia opcional
- [ ] Nada de lógica de dominio ni de producto adentro del kit
- [ ] Documentación actualizada si cambió la superficie pública
