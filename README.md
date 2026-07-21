# UniquexCR — Add-ons de Home Assistant

Repositorio privado de add-ons para el proyecto **UniquexCR / Fortunatta**.

## Add-ons

- **[UniquexCR Mirror](./ha_mirror/)** — Mirror FastAPI que conecta local a HA (token del
  Supervisor) y expone estado + control por WebSocket para el frontend de lujo.

## Instalar

Guía completa paso a paso (100% remota): **[`00_INSTALAR_REMOTO.md`](./00_INSTALAR_REMOTO.md)**.

Resumen: **Ajustes → Complementos → Tienda → ⋮ → Repositorios** → agregá la URL de este repo →
instalá **UniquexCR Mirror** → configurá `mirror_api_key` → Iniciar.

> Antes de publicar/actualizar: corré `package.ps1` para vendorizar el código del Mirror dentro del
> add-on.

## Seguridad

Este repo **no contiene secretos** — ni tokens, ni API keys, ni datos del cliente. Las claves se
configuran como opciones del add-on (tipo *password*) dentro de Home Assistant.
