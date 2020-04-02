import * as React from 'react';
import {
  ISourceDefinitionsListStore,
  ISourceDefintion,
} from '../../stores/sourceDefinitionsList';
import { inject, observer } from 'mobx-react';
import { Heading } from '@components/connections/styles';
import { HeaderDiv, LabelMedium } from '@components/common/typography';
import theme from '@css/theme';
import IconCardList from '@components/iconCardList';
import { Drawer } from 'antd';
import SourceConfigure from './sourcesConfigure/index';

export interface ISourcesCatalogueProps {
  sourceDefinitionsListStore?: ISourceDefinitionsListStore;
}

export interface ISourcesCatalogueState {
  modalVisible: boolean;
  selected?: ISourceDefintion;
}

@inject('sourceDefinitionsListStore')
@observer
export default class SourcesCatalogue extends React.Component<
  ISourcesCatalogueProps,
  ISourcesCatalogueState
> {
  constructor(props: ISourcesCatalogueProps) {
    super(props);
    this.state = {
      modalVisible: false,
    };
  }
  handleCancel = () => {
    this.setState({ modalVisible: false });
  };
  onClick = (sourceDef: any) => {
    // Add a modal and open it on click.
    this.setState({ modalVisible: true, selected: sourceDef });
  };

  public render() {
    const { sourceDefinitionsListStore } = this.props;
    const { selected } = this.state;
    if (sourceDefinitionsListStore)
      return (
        <div>
          <Drawer
            visible={this.state.modalVisible}
            onClose={this.handleCancel}
            width={'40%'}
          >
            <SourceConfigure sourceDef={selected} />
          </Drawer>
          <Heading>
            <HeaderDiv color={theme.color.primary}>Sources</HeaderDiv>
            <LabelMedium color={theme.color.grey300}>
              {sourceDefinitionsListStore!.sourceDefinitions.length}
              &nbsp;Available
            </LabelMedium>
          </Heading>
          <IconCardList
            type="source"
            selectionMode="none"
            icons={sourceDefinitionsListStore.sourceDefinitions.map(
              sourceDef => ({
                id: sourceDef.id,
                type: sourceDef.name,
                title: sourceDef.name,
                onClick: () => this.onClick(sourceDef),
              }),
            )}
            onSelectionChange={() => {}}
          />
        </div>
      );
  }
}
