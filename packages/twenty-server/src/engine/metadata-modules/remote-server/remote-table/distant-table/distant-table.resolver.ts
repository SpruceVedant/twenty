import { UseGuards } from '@nestjs/common';
import { Args, Query, Resolver } from '@nestjs/graphql';

import { Workspace } from 'src/engine/core-modules/workspace/workspace.entity';
import { AuthWorkspace } from 'src/engine/decorators/auth/auth-workspace.decorator';
import { JwtAuthGuard } from 'src/engine/guards/jwt.auth.guard';
import { DistantTableService } from 'src/engine/metadata-modules/remote-server/remote-table/distant-table/distant-table.service';
import { FindManyRemoteTablesInput } from 'src/engine/metadata-modules/remote-server/remote-table/dtos/find-many-remote-tables-input';
import { RemoteTableDTO } from 'src/engine/metadata-modules/remote-server/remote-table/dtos/remote-table.dto';

@UseGuards(JwtAuthGuard)
@Resolver()
export class DistantTableResolver {
  constructor(private readonly distantTableService: DistantTableService) {}

  @Query(() => [RemoteTableDTO])
  async findAvailableRemoteTablesByServerId(
    @Args('input') input: FindManyRemoteTablesInput,
    @AuthWorkspace() { id: workspaceId }: Workspace,
  ) {
    return this.distantTableService.findDistantTablesByServerId(
      input.id,
      workspaceId,
    );
  }
}
