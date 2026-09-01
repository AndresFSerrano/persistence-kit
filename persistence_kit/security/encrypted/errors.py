class EncryptedPayloadError(RuntimeError):
    """El sobre cifrado no se pudo abrir.

    Cubre tanto el sobre mal formado como el contenido que no pasa la
    verificación de integridad. En ambos casos el dato lo mandó un tercero,
    así que el borde HTTP lo traduce a un 400 y no a un 500.
    """
