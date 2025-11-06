package com.gabrielgollo.myApp.infrastructure.configs.lakeformation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class LakeFormationPermissionInitializer implements CommandLineRunner {

    private final LakeFormationService lakeFormationService;

    public LakeFormationPermissionInitializer(LakeFormationService lakeFormationService) {
        this.lakeFormationService = lakeFormationService;
    }

    @Override
    public void run(String... args) {
        try {
            lakeFormationService.ensureSelfPermissions();
            log.info("✅ Permissões Lake Formation aplicadas à role da aplicação.");
        } catch (Exception e) {
            log.info("❌ Falha ao aplicar permissões no Lake Formation: " + e.getMessage());
        }
    }
}
