<?php

declare(strict_types=1);

use Semitexa\Dev\Application\Service\Ai\Verify\Structure\LocalModuleStructureExtension;
use Semitexa\Dev\Application\Service\Ai\Verify\Structure\ModuleStructureRule;

if (!class_exists(LocalModuleStructureExtension::class) || !class_exists(ModuleStructureRule::class)) {
    return null;
}

return new LocalModuleStructureExtension(
    package: 'ledger',
    topLevelDirectories: [
        'Contract',
    ],
    pathRules: [
        'Contract' => new ModuleStructureRule(
            path: 'Contract',
            allowedFilePatterns: ['/^[A-Z][A-Za-z0-9_]*Interface\.php$/'],
            mode: ModuleStructureRule::MODE_LEAF_FILES_ONLY,
            rationale: 'semitexa-ledger-only: deprecated back-compat interface shims. The canonical interfaces live at Domain/Contract/; each file here is a class_alias from the old Semitexa\\Ledger\\Contract\\* FQCN to its Domain\\Contract\\* replacement. The shim MUST sit at src/Contract/<Name>.php so PSR-4 autoloads it when an external consumer still references the old FQCN (moving it would break that autoload). Interface-shim files only (*Interface.php), no subdirectories; deliberately kept (restored in 99e9a1a) for one release cycle.',
        ),
    ],
    reason: 'semitexa-ledger keeps a src/Contract/ back-compat shim namespace (class_alias of the pre-refactor Semitexa\\Ledger\\Contract\\ReplayHandlerInterface onto its canonical Domain\\Contract\\ replacement). It predates the structure spec relocation to Domain/Contract and is retained so external consumers of the old FQCN keep resolving via PSR-4.',
);
