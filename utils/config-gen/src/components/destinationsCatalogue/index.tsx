import * as React from 'react';
import {
  IDestinationDefsListStore,
  IDestinationDef,
} from '../../stores/destinationDefsList';
import { inject, observer } from 'mobx-react';
import { Heading } from '@components/connections/styles';
import { HeaderDiv, LabelMedium } from '@components/common/typography';
import theme from '@css/theme';
import IconCardList from '@components/iconCardList';
import { Drawer } from 'antd';
import DestinationConfigure from './destinationsConfigure/index';
import formTemplatesMap from '../destination/destinationSettings/destinationSettings.json';

export interface IDestinationsCatalogueProps {
  destinationDefsListStore?: IDestinationDefsListStore;
}

export interface IDestinationsCatalogueState {
  modalVisible: boolean;
  selected?: IDestinationDef;
  filteredDestinationDefs?: Array<object>;
}

@inject('destinationDefsListStore')
@observer
export default class DestinationsCatalogue extends React.Component<
  IDestinationsCatalogueProps,
  IDestinationsCatalogueState
> {
  constructor(props: IDestinationsCatalogueProps) {
    super(props);
    this.state = {
      modalVisible: false,
      filteredDestinationDefs: [],
    };
  }
  handleCancel = () => {
    this.setState({ modalVisible: false });
  };
  onClick = (destinationDef: any) => {
    // Add a modal and open it on click.
    this.setState({ modalVisible: true, selected: destinationDef });
  };

  componentDidMount() {
    const { destinationDefsListStore } = this.props;

    if (destinationDefsListStore) {
      const destinationDefs = destinationDefsListStore!.destinationDefs;
      const destinationSettingsArr = Object.keys(formTemplatesMap);
      const filteredArr = [] as Array<object>;
      destinationDefs.map(def => {
        if (destinationSettingsArr.includes(def.name)) {
          filteredArr.push(def);
        }
      });
      this.setState({ filteredDestinationDefs: filteredArr });
    }
  }

  public render() {
    const { destinationDefsListStore } = this.props;
    const { selected, filteredDestinationDefs } = this.state;
    if (destinationDefsListStore && filteredDestinationDefs) {
      return (
        <div>
          <Drawer
            visible={this.state.modalVisible}
            onClose={this.handleCancel}
            width={'40%'}
          >
            <DestinationConfigure destinationDef={selected} />
          </Drawer>
          <Heading>
            <HeaderDiv color={theme.color.primary}>Destinations</HeaderDiv>
            <LabelMedium color={theme.color.grey300}>
              {destinationDefsListStore!.destinationDefs.filter(dest => !dest.config.preview).length}
              &nbsp;Available
            </LabelMedium>
          </Heading>
          <IconCardList
            type="destination"
            selectionMode="none"
            icons={filteredDestinationDefs.map((destinationDef: any) => ({
              id: destinationDef.id,
              type: destinationDef.name,
              title: destinationDef.displayName,
              onClick: () => this.onClick(destinationDef),
            }))}
            onSelectionChange={() => {}}
          />
        </div>
      );
    }
  }
}
