import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

public class FileTransferRoute extends RouteBuilder {

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.configure().addRoutesBuilder(new FileTransferRoute());
        main.run(args);
    }

    @Override
    public void configure() throws Exception {

        from("file:input?noop=true")
            .routeId("ruta-ventas-a-inventario")
            .log("Archivo detectado: ${file:name}")
            .filter(header("CamelFileName").endsWith(".csv"))
                .log("Procesando archivo: ${file:name} - ${date:now:yyyy-MM-dd HH:mm:ss}")
                .to("direct:archivar")
                .convertBodyTo(String.class)
                .process(exchange -> {
                    String body = exchange.getIn().getBody(String.class);
                    String[] lineas = body.split("\\r?\\n");
                    StringBuilder sb = new StringBuilder(lineas[0]).append("\n");
                    for (int i = 1; i < lineas.length; i++) {
                        if (lineas[i].isBlank()) continue;
                        String[] col = lineas[i].split(",");
                        col[1] = col[1].toUpperCase();
                        sb.append(String.join(",", col)).append("\n");
                    }
                    exchange.getIn().setBody(sb.toString());
                })
                .to("file:output")
                .log("Archivo ${file:name} entregado en output (inventario)")
                .setBody(simple("${date:now:yyyy-MM-dd HH:mm:ss} | ${file:name} | procesado OK\n"))
                .to("file:logs?fileName=procesamiento.log&fileExist=Append")
            .end();

        from("direct:archivar")
            .routeId("ruta-archivado")
            .log("Archivando copia original de ${file:name}")
            .to("file:archived?fileName=${date:now:yyyyMMdd_HHmmss}_${file:name}");
    }
}