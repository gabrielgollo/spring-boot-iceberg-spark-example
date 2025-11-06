package com.gabrielgollo.myApp.infrastructure.configs.lakeformation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.*;
import software.amazon.awssdk.services.lakeformation.model.DataLakePrincipal;

@Slf4j
@Service
public class LakeFormationService {

    private final LakeFormationClient lfClient;

    public LakeFormationService() {
        this.lfClient = LakeFormationClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.US_EAST_1) // ajuste conforme necessário
                .build();
    }

    public void ensureSelfPermissions() {
        String roleArn = System.getenv("AWS_ROLE_ARN");
        if (roleArn == null || roleArn.isBlank()) {
            throw new IllegalStateException("Variável de ambiente AWS_ROLE_ARN não definida.");
        }

        String databaseName = "meu_database";
        String dataLocationArn = "arn:aws:s3:::meu-bucket-iceberg";

        // ✅ Usa DataLakePrincipal diretamente
        DataLakePrincipal principal = DataLakePrincipal.builder()
                .dataLakePrincipalIdentifier(roleArn)
                .build();

        // ✅ Database resource
        Resource dbResource = Resource.builder()
                .database(DatabaseResource.builder()
                        .name(databaseName)
                        .build())
                .build();

        GrantPermissionsRequest dbGrant = GrantPermissionsRequest.builder()
                .principal(principal)
                .resource(dbResource)
                .permissions(
                        Permission.SELECT,
                        Permission.DESCRIBE,
                        Permission.ALTER,
                        Permission.DROP
                )
                .build();

        lfClient.grantPermissions(dbGrant);

        // ✅ Data location (para Iceberg no S3)
        Resource locationResource = Resource.builder()
                .dataLocation(DataLocationResource.builder()
                        .resourceArn(dataLocationArn)
                        .build())
                .build();

        GrantPermissionsRequest locationGrant = GrantPermissionsRequest.builder()
                .principal(principal)
                .resource(locationResource)
                .permissions(Permission.DATA_LOCATION_ACCESS)
                .build();

        lfClient.grantPermissions(locationGrant);

        log.info("Permissões concedidas à própria role: {}", roleArn);
    }
}